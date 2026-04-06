import streamlit as st
import pandas as pd
import datetime
import calendar
import io
import re
import time
import urllib.parse
import pickle
import json
import base64
from pathlib import Path
import requests
from bs4 import BeautifulSoup
from openai import OpenAI

# ──────────────────────────────────────────────
# 페이지 기본 설정
# ──────────────────────────────────────────────
st.set_page_config(
    page_title="신한카드 SA 캠페인 관리 대시보드",
    page_icon="📊",
    layout="wide",
)

# ──────────────────────────────────────────────
# 메뉴 정의
# ──────────────────────────────────────────────
MENU_ITEMS = {
    "1. 요약 및 예산": "📋 요약 및 예산",
    "2. 키워드 Top10": "🔑 키워드 Top10",
    "3. 미노출 현황": "🚫 미노출 현황",
    "4. 급상승·급하락 키워드": "📈 급상승·급하락 키워드",
    "5. 경쟁사 쿼리 추이": "🏢 경쟁사 쿼리 추이",
    "6. 주간 AI 코멘트": "🤖 주간 AI 코멘트",
    "7. 미디어믹스 제안": "💡 미디어믹스 제안",
}

# ──────────────────────────────────────────────
# 캐시 디렉토리 (로컬 디스크 영속성)
# ──────────────────────────────────────────────
CACHE_DIR = Path(__file__).parent / ".cache"
CACHE_DIR.mkdir(exist_ok=True)

def _save_pickle(name, obj):
    with open(CACHE_DIR / f"{name}.pkl", "wb") as f:
        pickle.dump(obj, f)

def _load_pickle(name, default=None):
    p = CACHE_DIR / f"{name}.pkl"
    if p.exists():
        try:
            with open(p, "rb") as f:
                return pickle.load(f)
        except Exception:
            pass
    return default

# ──────────────────────────────────────────────
# GitHub API 영속성 (Cloud 리부트 대응)
# ──────────────────────────────────────────────
_GH_TOKEN = None
_GH_REPO = "joonee-web/shinhan"
try:
    _GH_TOKEN = st.secrets["GITHUB_TOKEN"]
    _GH_REPO = st.secrets.get("GITHUB_REPO", _GH_REPO) if hasattr(st.secrets, "get") else _GH_REPO
except Exception:
    pass

_GH_ENABLED = bool(_GH_TOKEN)
_GH_HEADERS = {"Authorization": f"token {_GH_TOKEN}", "Accept": "application/vnd.github+json"} if _GH_ENABLED else {}

def _gh_get(path):
    """GitHub에서 파일 내용과 SHA를 가져온다. 1MB 초과 파일은 download_url 사용."""
    if not _GH_ENABLED:
        return None, None
    url = f"https://api.github.com/repos/{_GH_REPO}/contents/{path}"
    try:
        r = requests.get(url, headers=_GH_HEADERS, timeout=15)
        if r.status_code != 200:
            return None, None
        data = r.json()
        sha = data.get("sha")
        # 1MB 이하: content 필드에 base64 데이터 포함
        if data.get("content"):
            return base64.b64decode(data["content"]), sha
        # 1MB 초과: download_url로 직접 다운로드
        dl_url = data.get("download_url")
        if dl_url:
            dl = requests.get(dl_url, headers=_GH_HEADERS, timeout=60)
            if dl.status_code == 200:
                return dl.content, sha
    except Exception:
        pass
    return None, None

def _gh_put(path, content_bytes, message="auto-save"):
    """GitHub에 파일을 생성/업데이트한다. 409 충돌 시 재시도."""
    if not _GH_ENABLED:
        return False, "GitHub 미연결"
    url = f"https://api.github.com/repos/{_GH_REPO}/contents/{path}"
    encoded = base64.b64encode(content_bytes).decode()
    for attempt in range(3):
        try:
            r = requests.get(url, headers=_GH_HEADERS, timeout=15)
            body = {"message": message, "content": encoded}
            if r.status_code == 200:
                body["sha"] = r.json()["sha"]
            resp = requests.put(url, headers=_GH_HEADERS, json=body, timeout=60)
            if resp.status_code in (200, 201):
                return True, ""
            if resp.status_code == 409 and attempt < 2:
                time.sleep(1)
                continue
            return False, f"{path}: HTTP {resp.status_code} - {resp.text[:200]}"
        except Exception as e:
            if attempt < 2:
                time.sleep(1)
                continue
            return False, f"{path}: {e}"
    return False, f"{path}: 재시도 초과"

def _gh_save_data(name, obj):
    """pickle+base64로 GitHub에 저장."""
    data = pickle.dumps(obj)
    ok, err = _gh_put(f"_persistence/{name}.pkl", data, f"save {name}")
    return ok, err

def _gh_load_data(name):
    """GitHub에서 pickle 데이터를 로드."""
    content, _ = _gh_get(f"_persistence/{name}.pkl")
    if content:
        return pickle.loads(content)
    return None

def save_all_cache():
    """현재 session_state의 주요 데이터를 디스크 + GitHub에 저장."""
    # 로컬 디스크
    _save_pickle("naver_data", st.session_state.naver_data)
    _save_pickle("google_data", st.session_state.google_data)
    _save_pickle("naver_mapping_rules", st.session_state.naver_mapping_rules)
    _save_pickle("google_mapping_rules", st.session_state.google_mapping_rules)
    _save_pickle("campaign_categories", st.session_state.campaign_categories)
    _save_pickle("budgets", st.session_state.budgets)
    _save_pickle("device_ratios", st.session_state.device_ratios)
    # GitHub (Cloud 영속성)
    if _GH_ENABLED:
        errors = []
        for key in ["naver_data", "google_data", "naver_mapping_rules",
                     "google_mapping_rules", "campaign_categories", "budgets", "device_ratios"]:
            val = getattr(st.session_state, key, None)
            if val is None:
                continue
            if isinstance(val, pd.DataFrame) and val.empty:
                continue
            if isinstance(val, (dict, list)) and not val:
                continue
            ok, err = _gh_save_data(key, val)
            if not ok:
                errors.append(err)
        if not errors:
            st.toast("☁️ GitHub 저장 완료")
        else:
            st.toast(f"⚠️ GitHub 저장 실패: {errors[0]}", icon="⚠️")

def load_from_github():
    """GitHub에서 데이터 복원 (Cloud 리부트 후 첫 로드)."""
    if not _GH_ENABLED:
        return
    restored = []
    failed = []
    for key in ["campaign_categories", "budgets", "device_ratios",
                 "naver_mapping_rules", "google_mapping_rules",
                 "naver_data", "google_data"]:
        try:
            val = _gh_load_data(key)
            if val is not None:
                st.session_state[key] = val
                restored.append(key)
            else:
                failed.append(key)
        except Exception as e:
            failed.append(f"{key}({e})")
    if restored:
        st.toast(f"☁️ GitHub 복원 완료: {', '.join(restored)}")
    if failed:
        st.toast(f"⚠️ GitHub 복원 실패: {', '.join(failed)}", icon="⚠️")

# ──────────────────────────────────────────────
# session_state 초기화 (캐시에서 복원)
# ──────────────────────────────────────────────
if "campaign_categories" not in st.session_state:
    st.session_state.campaign_categories = _load_pickle(
        "campaign_categories", ["브랜드", "신용카드", "체크카드", "대출", "보험"]
    )

if "naver_data" not in st.session_state:
    st.session_state.naver_data = _load_pickle("naver_data", None)

if "google_data" not in st.session_state:
    st.session_state.google_data = _load_pickle("google_data", None)

if "budgets" not in st.session_state:
    st.session_state.budgets = _load_pickle("budgets", {})

if "device_ratios" not in st.session_state:
    st.session_state.device_ratios = _load_pickle("device_ratios", {})

if "naver_mapping_rules" not in st.session_state:
    _cached_naver = _load_pickle("naver_mapping_rules", None)
    if _cached_naver is not None:
        st.session_state.naver_mapping_rules = _cached_naver
    else:
        st.session_state.naver_mapping_rules = pd.DataFrame([
            {"캠페인_포함": "브랜드키워드", "광고그룹_포함": "",              "캠페인구분": "브랜드"},
            {"캠페인_포함": "고객서비스",   "광고그룹_포함": "",              "캠페인구분": "브랜드"},
            {"캠페인_포함": "신한법인",     "광고그룹_포함": "",              "캠페인구분": "브랜드"},
            {"캠페인_포함": "신한쏠페이",   "광고그룹_포함": "",              "캠페인구분": "브랜드"},
            {"캠페인_포함": "기타서비스",   "광고그룹_포함": "",              "캠페인구분": "브랜드"},
            {"캠페인_포함": "나라사랑",     "광고그룹_포함": "",              "캠페인구분": "신용카드"},
            {"캠페인_포함": "마이신한포인트","광고그룹_포함": "",             "캠페인구분": "브랜드"},
            {"캠페인_포함": "BizPlan",     "광고그룹_포함": "",              "캠페인구분": "신용카드"},
            {"캠페인_포함": "BSA",         "광고그룹_포함": "",              "캠페인구분": "브랜드"},
            {"캠페인_포함": "소상공인",     "광고그룹_포함": "",              "캠페인구분": "브랜드"},
            {"캠페인_포함": "",            "광고그룹_포함": "대출",          "캠페인구분": "대출"},
            {"캠페인_포함": "",            "광고그룹_포함": "신한카드",      "캠페인구분": "브랜드"},
        ])
        st.session_state._naver_rules_default = True

if "google_mapping_rules" not in st.session_state:
    _cached_google = _load_pickle("google_mapping_rules", None)
    if _cached_google is not None:
        st.session_state.google_mapping_rules = _cached_google
    else:
        st.session_state.google_mapping_rules = pd.DataFrame([
            {"캠페인_포함": "브랜드키워드", "광고그룹_포함": "",              "캠페인구분": "브랜드"},
            {"캠페인_포함": "고객서비스",   "광고그룹_포함": "",              "캠페인구분": "브랜드"},
            {"캠페인_포함": "신한쏠페이",   "광고그룹_포함": "",              "캠페인구분": "브랜드"},
            {"캠페인_포함": "나라사랑",     "광고그룹_포함": "",              "캠페인구분": "신용카드"},
            {"캠페인_포함": "소상공인",     "광고그룹_포함": "",              "캠페인구분": "브랜드"},
            {"캠페인_포함": "신한카드법인", "광고그룹_포함": "",              "캠페인구분": "브랜드"},
        ])
        st.session_state._google_rules_default = True

# ── GitHub에서 복원 (Cloud 리부트 후 첫 로드) ──
if "_gh_restored" not in st.session_state:
    load_from_github()
    st.session_state._gh_restored = True

# ──────────────────────────────────────────────
# 유틸리티 함수
# ──────────────────────────────────────────────
def find_col(df, candidates):
    """컬럼명 후보 리스트에서 df에 존재하는 첫 번째 컬럼을 반환."""
    for c in candidates:
        for col in df.columns:
            if c == col.strip():
                return col
    return None


def read_naver_csv(file):
    """네이버 SA CSV 읽기 (첫 행 제목 스킵)."""
    if file.name.endswith(".csv"):
        return pd.read_csv(file, skiprows=1, encoding="utf-8-sig")
    return pd.read_excel(file, skiprows=1)


def read_google_csv(file):
    """구글 SA CSV 읽기 (처음 2행 스킵, 탭 구분, UTF-16)."""
    if file.name.endswith(".csv"):
        try:
            return pd.read_csv(file, sep="\t", skiprows=2, encoding="utf-16")
        except Exception:
            file.seek(0)
            return pd.read_csv(file, sep="\t", skiprows=2, encoding="utf-8-sig")
    return pd.read_excel(file, skiprows=2)


def merge_data(existing, new_df, key_cols):
    """기존 데이터에 새 데이터를 누적. 키 컬럼 중복 시 새 데이터로 덮어쓰기."""
    if existing is None or existing.empty:
        return new_df
    available_keys = [c for c in key_cols if c in new_df.columns and c in existing.columns]
    if not available_keys:
        return pd.concat([existing, new_df], ignore_index=True)
    combined = pd.concat([existing, new_df], ignore_index=True)
    combined = combined.drop_duplicates(subset=available_keys, keep="last")
    return combined.reset_index(drop=True)


def apply_mapping_rules(df, mapping_rules):
    """매핑 규칙 DataFrame을 사용하여 캠페인구분 컬럼 생성 (벡터화)."""
    result = pd.Series("기타", index=df.index)
    assigned = pd.Series(False, index=df.index)
    camp_str = df["캠페인"].astype(str)
    adgrp_str = df["광고그룹"].astype(str)
    for _, rule in mapping_rules.iterrows():
        c_key = str(rule["캠페인_포함"]).strip()
        a_key = str(rule["광고그룹_포함"]).strip()
        if not c_key and not a_key:
            continue
        c_ok = camp_str.str.contains(c_key, na=False, regex=False) if c_key else True
        a_ok = adgrp_str.str.contains(a_key, na=False, regex=False) if a_key else True
        mask = c_ok & a_ok & ~assigned
        result[mask] = rule["캠페인구분"]
        assigned = assigned | mask
    df["캠페인구분"] = result
    return df


def map_device_vec(camp_series):
    """캠페인명 Series에서 디바이스(PC/MO) 추출 (벡터화)."""
    upper = camp_series.astype(str).str.upper()
    device = pd.Series("통합", index=camp_series.index)
    pc_mask = upper.str.contains("_PC", na=False) | upper.str.startswith("PC")
    mo_mask = upper.str.contains("_MO", na=False) | upper.str.contains("_M_", na=False) | upper.str.startswith("MO")
    device[pc_mask] = "PC"
    device[mo_mask] = "MO"
    return device


def _build_naver_session(device="PC"):
    """네이버 검색용 requests.Session 생성 (쿠키·리다이렉트 자동 처리)."""
    s = requests.Session()
    if device == "PC":
        s.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/131.0.0.0 Safari/537.36"
            ),
            "Referer": "https://www.naver.com/",
        })
    else:
        s.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Linux; Android 14; SM-S911B) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/131.0.0.0 Mobile Safari/537.36"
            ),
            "Referer": "https://m.naver.com/",
        })
    s.headers.update({
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Cache-Control": "max-age=0",
    })
    return s


# 세션 재사용으로 쿠키 유지
_pc_session = None
_mo_session = None


def check_naver_ad_exposure(keyword, device="PC"):
    """네이버 검색에서 shinhancard.com 광고 노출 여부를 확인한다."""
    global _pc_session, _mo_session
    encoded = urllib.parse.quote(keyword)

    if device == "PC":
        url = f"https://search.naver.com/search.naver?where=nexearch&query={encoded}"
        if _pc_session is None:
            _pc_session = _build_naver_session("PC")
        session = _pc_session
    else:  # MO
        url = f"https://m.search.naver.com/search.naver?where=m&query={encoded}"
        if _mo_session is None:
            _mo_session = _build_naver_session("MO")
        session = _mo_session

    for attempt in range(2):
        try:
            resp = session.get(url, timeout=15, allow_redirects=True)
            resp.raise_for_status()
            html = resp.text
            soup = BeautifulSoup(html, "html.parser")

            # 1) 광고 영역 CSS 선택자로 탐색
            ad_selectors = [
                "[class*='ad_area']", "[class*='power_link']",
                "[class*='lst_type']", "[data-cr-area*='psa']",
                "[data-cr-area*='nsa']", "[class*='sponsor']",
                "[class*='ca_ad']", "[class*='_ad']",
            ]
            for sel in ad_selectors:
                for el in soup.select(sel):
                    if "shinhancard.com" in el.get_text().lower():
                        return "노출"

            # 2) shinhancard.com 링크가 광고 맥락에 있는지 확인
            for a_tag in soup.find_all("a", href=True):
                href = a_tag.get("href", "")
                text = a_tag.get_text()
                if "shinhancard.com" in href or "shinhancard.com" in text.lower():
                    parent = a_tag.parent
                    for _ in range(6):
                        if parent is None:
                            break
                        p_text = parent.get_text() if hasattr(parent, "get_text") else ""
                        p_cls = " ".join(parent.get("class", [])) if hasattr(parent, "get") else ""
                        if "광고" in p_text or "ad" in p_cls.lower() or "sponsor" in p_cls.lower():
                            return "노출"
                        parent = parent.parent

            # 3) 페이지 전체에서 shinhancard.com 존재 여부 (폴백)
            if "shinhancard.com" in html:
                return "노출"

            return "미노출"
        except (requests.exceptions.RequestException, Exception):
            if attempt == 0:
                # 세션 재생성 후 재시도
                if device == "PC":
                    _pc_session = _build_naver_session("PC")
                    session = _pc_session
                else:
                    _mo_session = _build_naver_session("MO")
                    session = _mo_session
                time.sleep(0.5)
                continue
            return "요청오류"
    return "요청오류"


_PREP_VERSION = 2  # 주차 로직 변경 시 버전 올려 캐시 무효화

@st.cache_data(show_spinner="네이버 데이터 처리 중...")
def prepare_naver_data(df, mapping_rules_json, _version=_PREP_VERSION):
    """네이버 SA 데이터 정제: 매핑·디바이스·주차·비용 컬럼 추가."""
    df = df.copy()
    mapping_rules = pd.read_json(io.StringIO(mapping_rules_json))
    required = ["캠페인", "광고그룹", "일별", "노출수", "클릭수", "총비용"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        return None, f"필수 컬럼 누락: {', '.join(missing)}"

    df = apply_mapping_rules(df, mapping_rules)
    df["디바이스"] = map_device_vec(df["캠페인"])

    df["_date"] = pd.to_datetime(df["일별"], errors="coerce")
    # 월~일 기준 주차: 해당 월 첫 번째 월요일 기준으로 주차 산정 (첫 월요일 전 = W0)
    _first_day = df["_date"].dt.to_period("M").dt.to_timestamp()
    _first_mon_offset = (7 - _first_day.dt.dayofweek) % 7
    _first_mon = _first_day + pd.to_timedelta(_first_mon_offset, unit="D")
    _diff = (df["_date"] - _first_mon).dt.days
    _wn = _diff // 7 + 1
    _wn = _wn.where(_diff >= 0, 0)  # 첫 월요일 전 날짜는 W0
    df["주차"] = "W" + _wn.astype("Int64").astype(str)

    df["노출수"] = pd.to_numeric(df["노출수"], errors="coerce").fillna(0).astype(int)
    df["클릭수"] = pd.to_numeric(df["클릭수"], errors="coerce").fillna(0).astype(int)
    df["RAW비용"] = pd.to_numeric(df["총비용"], errors="coerce").fillna(0)

    # 네이버: 총비용=RAW, CPC/CPM 기준=RAW/1.1
    df["비용"] = df["RAW비용"]
    df["CPC기준비용"] = df["RAW비용"] / 1.1
    df["매체"] = "네이버"
    return df, None


@st.cache_data(show_spinner="구글 데이터 처리 중...")
def prepare_google_data(df, mapping_rules_json, _version=_PREP_VERSION):
    """구글 SA 데이터 정제: 매핑·주차·비용 컬럼 추가."""
    df = df.copy()
    mapping_rules = pd.read_json(io.StringIO(mapping_rules_json))
    required = ["캠페인", "광고그룹", "일", "노출수", "클릭수", "비용"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        return None, f"필수 컬럼 누락: {', '.join(missing)}"

    df = apply_mapping_rules(df, mapping_rules)
    df["디바이스"] = "통합"

    df["_date"] = pd.to_datetime(df["일"], errors="coerce")
    # 월~일 기준 주차: 해당 월 첫 번째 월요일 기준으로 주차 산정 (첫 월요일 전 = W0)
    _first_day = df["_date"].dt.to_period("M").dt.to_timestamp()
    _first_mon_offset = (7 - _first_day.dt.dayofweek) % 7
    _first_mon = _first_day + pd.to_timedelta(_first_mon_offset, unit="D")
    _diff = (df["_date"] - _first_mon).dt.days
    _wn = _diff // 7 + 1
    _wn = _wn.where(_diff >= 0, 0)
    df["주차"] = "W" + _wn.astype("Int64").astype(str)

    df["노출수"] = pd.to_numeric(df["노출수"], errors="coerce").fillna(0).astype(int)
    df["클릭수"] = pd.to_numeric(df["클릭수"], errors="coerce").fillna(0).astype(int)
    df["RAW비용"] = pd.to_numeric(df["비용"], errors="coerce").fillna(0)

    # 구글: 총비용=RAW*1.1*1.1, CPC/CPM 기준=RAW
    df["비용"] = df["RAW비용"] * 1.1 * 1.1
    df["CPC기준비용"] = df["RAW비용"]
    df["매체"] = "구글"
    return df, None


# ──────────────────────────────────────────────
# 사이드바
# ──────────────────────────────────────────────
with st.sidebar:
    # ── GitHub 연결 상태 ──
    if _GH_ENABLED:
        st.caption("☁️ GitHub 자동 저장: **활성**")
    else:
        st.caption("💾 로컬 저장 모드 (GitHub 미연결)")

    # ── 캠페인 관리 섹션 ──
    st.header("📂 캠페인 관리")

    # 캠페인 구분(카테고리) 추가
    with st.expander("캠페인 구분 추가 / 삭제", expanded=False):
        new_category = st.text_input("새 캠페인 구분 입력", key="new_cat_input")
        if st.button("➕ 추가", width="stretch"):
            if new_category and new_category not in st.session_state.campaign_categories:
                st.session_state.campaign_categories.append(new_category)
                save_all_cache()
                st.success(f"'{new_category}' 추가 완료")
                st.rerun()
            elif new_category in st.session_state.campaign_categories:
                st.warning("이미 존재하는 구분입니다.")
            else:
                st.warning("구분명을 입력해 주세요.")

        st.caption("**현재 캠페인 구분 목록**")
        for i, cat in enumerate(st.session_state.campaign_categories):
            col_name, col_btn = st.columns([3, 1])
            col_name.write(f"{i+1}. {cat}")
            if col_btn.button("✖", key=f"del_cat_{i}"):
                st.session_state.campaign_categories.remove(cat)
                save_all_cache()
                st.rerun()

    # ── 네이버 SA 캠페인 구분 매핑 규칙 ──
    with st.expander("네이버 SA 매핑 규칙 설정", expanded=False):
        st.caption(
            "캠페인·광고그룹 텍스트 **포함** 여부로 캠페인구분을 매핑합니다.\n"
            "빈 칸은 조건 없음(모두 매칭). 위에서부터 순서대로 적용됩니다."
        )
        cat_options = st.session_state.campaign_categories + ["기타"]
        edited_naver_rules = st.data_editor(
            st.session_state.naver_mapping_rules,
            num_rows="dynamic",
            width="stretch",
            column_config={
                "캠페인구분": st.column_config.SelectboxColumn(
                    "캠페인구분", options=cat_options, required=True,
                ),
            },
            key="naver_mapping_editor",
        )
        if st.button("💾 네이버 매핑 저장", width="stretch"):
            st.session_state.naver_mapping_rules = edited_naver_rules.copy()
            save_all_cache()
            st.toast("✅ 네이버 매핑 규칙이 저장되었습니다.")
            st.rerun()

    # ── 구글 SA 캠페인 구분 매핑 규칙 ──
    with st.expander("구글 SA 매핑 규칙 설정", expanded=False):
        st.caption(
            "캠페인·광고그룹 텍스트 **포함** 여부로 캠페인구분을 매핑합니다.\n"
            "빈 칸은 조건 없음(모두 매칭). 위에서부터 순서대로 적용됩니다."
        )
        cat_options_g = st.session_state.campaign_categories + ["기타"]
        edited_google_rules = st.data_editor(
            st.session_state.google_mapping_rules,
            num_rows="dynamic",
            width="stretch",
            column_config={
                "캠페인구분": st.column_config.SelectboxColumn(
                    "캠페인구분", options=cat_options_g, required=True,
                ),
            },
            key="google_mapping_editor",
        )
        if st.button("💾 구글 매핑 저장", width="stretch"):
            st.session_state.google_mapping_rules = edited_google_rules.copy()
            save_all_cache()
            st.toast("✅ 구글 매핑 규칙이 저장되었습니다.")
            st.rerun()

    st.divider()

    # ── 메뉴 선택 ──
    st.header("📌 메뉴")
    selected_menu = st.radio(
        "이동할 메뉴를 선택하세요",
        list(MENU_ITEMS.keys()),
        label_visibility="collapsed",
    )

    st.divider()

    # ── RAW 데이터 업로드 ──
    st.header("📤 RAW 데이터 업로드")

    st.subheader("네이버 SA", divider=False)
    naver_file = st.file_uploader(
        "네이버 SA RAW 데이터 (캠페인에 '0.' 또는 '자사명' 포함 데이터)",
        type=["csv", "xlsx", "xls"],
        key="naver_uploader",
    )
    if naver_file is not None:
        try:
            raw = read_naver_csv(naver_file)
            mask = raw["캠페인"].str.contains("0.", na=False) | raw["캠페인"].str.contains("자사명", na=False)
            raw = raw[mask]
            naver_keys = ["일별", "캠페인", "광고그룹", "키워드"]
            st.session_state.naver_data = merge_data(st.session_state.naver_data, raw, naver_keys)
            save_all_cache()
            total = len(st.session_state.naver_data)
            st.success(f"네이버 데이터 누적 완료 (신규 {len(raw)}행 → 총 {total}행)")
        except Exception as e:
            st.error(f"네이버 데이터 로드 실패: {e}")

    st.subheader("구글 SA", divider=False)
    google_file = st.file_uploader(
        "구글 SA RAW 데이터 (캠페인에 '0.' 포함 데이터)",
        type=["csv", "xlsx", "xls"],
        key="google_uploader",
    )
    if google_file is not None:
        try:
            raw = read_google_csv(google_file)
            raw = raw[raw["캠페인"].str.contains("0.", na=False) & ~raw["캠페인"].str.contains("GSA", na=False)]
            google_keys = ["일", "캠페인", "광고그룹", "키워드"]
            st.session_state.google_data = merge_data(st.session_state.google_data, raw, google_keys)
            save_all_cache()
            total = len(st.session_state.google_data)
            st.success(f"구글 데이터 누적 완료 (신규 {len(raw)}행 → 총 {total}행)")
        except Exception as e:
            st.error(f"구글 데이터 로드 실패: {e}")

# ──────────────────────────────────────────────
# 메인 화면
# ──────────────────────────────────────────────
page_title = MENU_ITEMS[selected_menu]
st.title(f"신한카드 SA 캠페인 관리 대시보드")
st.header(page_title)
st.divider()

# 각 메뉴별 뼈대 영역
menu_key = selected_menu.split(". ", 1)[1]

if menu_key == "요약 및 예산":
    has_naver = st.session_state.naver_data is not None
    has_google = st.session_state.google_data is not None

    if not has_naver and not has_google:
        st.info("📋 사이드바에서 네이버 또는 구글 SA RAW 데이터를 업로드해 주세요.")
    else:
        # ── 연도/월 선택 ──
        today = datetime.date.today()
        col_y, col_m, _ = st.columns([1, 1, 4])
        sel_year = col_y.selectbox("연도", list(range(today.year - 1, today.year + 2)), index=1, key="sel_year")
        sel_month = col_m.selectbox("월", list(range(1, 13)), index=today.month - 1, key="sel_month")
        ym_key = f"{sel_year}-{sel_month:02d}"
        days_in_month = calendar.monthrange(sel_year, sel_month)[1]
        if sel_year == today.year and sel_month == today.month:
            progress_rate = (today.day - 1) / days_in_month
        elif datetime.date(sel_year, sel_month, 1) < today:
            progress_rate = 1.0
        else:
            progress_rate = 0.0

        st.divider()

        naver_rules_json = st.session_state.naver_mapping_rules.to_json()
        google_rules_json = st.session_state.google_mapping_rules.to_json()
        frames = []

        # 네이버
        if st.session_state.naver_data is not None:
            ndf, err = prepare_naver_data(st.session_state.naver_data, naver_rules_json)
            if err:
                st.error(f"[네이버] {err}")
            else:
                frames.append(ndf)

        # 구글
        if st.session_state.google_data is not None:
            gdf, err = prepare_google_data(st.session_state.google_data, google_rules_json)
            if err:
                st.error(f"[구글] {err}")
            else:
                frames.append(gdf)

        if not frames:
            st.stop()

        combined = pd.concat(frames, ignore_index=True)

        # 선택한 연도/월로 필터
        month_filter = (
            (combined["_date"].dt.year == sel_year)
            & (combined["_date"].dt.month == sel_month)
        )
        combined = combined[month_filter]

        if combined.empty:
            st.warning(f"{sel_year}년 {sel_month}월 데이터가 없습니다.")
            st.stop()

        # ── 비용 Summary ──
        st.subheader(f"📋 매체별 비용 Summary ({sel_year}년 {sel_month}월)")

        week_num = combined["주차"].str.extract(r"(\d+)").astype("Int64")[0]
        combined["_week_num"] = week_num

        for media, mgrp in combined.groupby("매체"):
            st.markdown(f"#### {media}")

            summary_grp = mgrp[mgrp["캠페인구분"] != "기타"]
            if summary_grp.empty:
                st.caption("해당 데이터 없음")
                continue
            pivot_wide = summary_grp.pivot_table(
                index="캠페인구분",
                columns="_week_num",
                values="비용",
                aggfunc="sum",
                fill_value=0,
            )
            pivot_wide.columns = [str(int(c)) for c in pivot_wide.columns]
            pivot_wide["총계"] = pivot_wide.sum(axis=1)
            pivot_wide = pivot_wide.sort_index()

            grand = pivot_wide.sum(numeric_only=True)
            grand.name = "총계"
            pivot_display = pd.concat([pivot_wide, grand.to_frame().T])

            fmt = {col: "₩{:,.0f}" for col in pivot_display.columns}
            st.dataframe(
                pivot_display.style.format(fmt, na_rep=""),
                width="stretch",
                height=min(len(pivot_display) * 38 + 50, 600),
            )

        st.divider()

        # ── 예산 입력 섹션 ──
        st.subheader(f"💰 매체별 · 캠페인 구분별 월 예산 ({ym_key})")

        available_media = sorted(combined["매체"].unique())
        sel_media = st.radio("매체 선택", available_media, horizontal=True, key="budget_media_sel")

        cats = sorted(combined[combined["매체"] == sel_media]["캠페인구분"].unique())
        cats = [c for c in cats if c != "기타"]

        budget_dict = {}
        n_cols = min(len(cats), 4) or 1
        budget_cols = st.columns(n_cols)
        for idx, cat in enumerate(cats):
            bkey = f"{ym_key}_{sel_media}_{cat}"
            stored = st.session_state.budgets.get(bkey, 0)
            with budget_cols[idx % n_cols]:
                val = st.number_input(
                    f"{cat}",
                    min_value=0,
                    value=stored,
                    step=100_000,
                    format="%d",
                    key=f"budget_{bkey}",
                )
                st.session_state.budgets[bkey] = val
                budget_dict[f"{sel_media}_{cat}"] = val

        # ── 네이버 디바이스 비중 입력 ──
        if sel_media == "네이버":
            st.markdown("##### 📱 네이버 디바이스별 예산 비중")
            st.caption("PC와 MO 비중을 숫자로 입력하세요. (예: PC 7, MO 3 → PC 70%, MO 30%)")
            ratio_cols = st.columns(len(cats) if cats else 1)
            for idx, cat in enumerate(cats):
                rkey = f"{ym_key}_{cat}"
                stored_r = st.session_state.device_ratios.get(rkey, {"PC": 5, "MO": 5})
                with ratio_cols[idx % len(ratio_cols)]:
                    st.markdown(f"**{cat}**")
                    rc1, rc2 = st.columns(2)
                    pc_val = rc1.number_input("PC", min_value=0, value=stored_r.get("PC", 5), step=1, key=f"ratio_pc_{rkey}")
                    mo_val = rc2.number_input("MO", min_value=0, value=stored_r.get("MO", 5), step=1, key=f"ratio_mo_{rkey}")
                    st.session_state.device_ratios[rkey] = {"PC": pc_val, "MO": mo_val}
                    total_r = pc_val + mo_val
                    if total_r > 0:
                        st.caption(f"PC {pc_val/total_r:.0%} / MO {mo_val/total_r:.0%}")

        save_all_cache()
        st.divider()

        # ── 피벗 테이블 ──
        st.subheader("📊 주차별 성과 피벗 테이블")

        sel_media_pivot = st.radio("매체 선택", available_media, horizontal=True, key="pivot_media_sel")

        # ── 디바이스 선택 (네이버) ──
        sel_device = "통합"
        if sel_media_pivot == "네이버":
            sel_device = st.radio("디바이스 선택", ["통합", "PC", "MO"], horizontal=True, key="pivot_device_sel")

        # ── 기준일 정보 ──
        yesterday = today - datetime.timedelta(days=1)
        if sel_year == today.year and sel_month == today.month:
            yesterday_progress = (today.day - 1) / days_in_month
        elif datetime.date(sel_year, sel_month, 1) < today:
            yesterday_progress = 1.0
        else:
            yesterday_progress = 0.0
        ic1, ic2 = st.columns(2)
        ic1.metric("📅 기준일 (어제)", yesterday.strftime("%Y-%m-%d"))
        ic2.metric("📈 기간 진척율 (어제 기준)", f"{yesterday_progress:.1%}")

        pivot_data = combined[(combined["매체"] == sel_media_pivot) & (combined["캠페인구분"] != "기타")]
        if sel_media_pivot == "네이버" and sel_device != "통합":
            pivot_data = pivot_data[pivot_data["캠페인"].str.contains(sel_device, na=False)]

        # ── 캠페인구분별 전체 요약 ──
        summary_agg = (
            pivot_data.groupby("캠페인구분")
            .agg(
                노출수=("노출수", "sum"),
                클릭수=("클릭수", "sum"),
                비용=("비용", "sum"),
                CPC기준비용=("CPC기준비용", "sum"),
            )
            .sort_index()
        )
        summary_agg["CTR(%)"] = (summary_agg["클릭수"] / summary_agg["노출수"].replace(0, float("nan")) * 100).round(2)
        summary_agg["CPC"] = (summary_agg["CPC기준비용"] / summary_agg["클릭수"].replace(0, float("nan"))).round(0)
        summary_agg["CPM"] = (summary_agg["CPC기준비용"] / summary_agg["노출수"].replace(0, float("nan")) * 1000).round(0)

        # 예산·소진율·액션 컬럼 추가
        def _get_device_budget(cat_name, full_budget):
            """디바이스 선택 시 비중에 따른 예산 계산."""
            if sel_media_pivot != "네이버" or sel_device == "통합":
                return full_budget
            rkey = f"{ym_key}_{cat_name}"
            ratios = st.session_state.device_ratios.get(rkey, {"PC": 5, "MO": 5})
            total_r = ratios.get("PC", 5) + ratios.get("MO", 5)
            if total_r == 0:
                return full_budget * 0.5
            return full_budget * ratios.get(sel_device, 5) / total_r

        budgets_list = []
        burn_list = []
        actions = []
        if sel_media_pivot == "네이버" and sel_device != "통합":
            label = f"입찰가({sel_device})"
        elif sel_media_pivot == "네이버":
            label = "입찰가"
        else:
            label = "일예산"
        for cat_name in summary_agg.index:
            bk = f"{ym_key}_{sel_media_pivot}_{cat_name}"
            full_b = st.session_state.budgets.get(bk, 0)
            b = _get_device_budget(cat_name, full_b)
            budgets_list.append(b)
            if b > 0:
                cat_cost = summary_agg.loc[cat_name, "비용"]
                br = cat_cost / b
                burn_list.append(br)
                if br < progress_rate:
                    actions.append("🔵 상향")
                elif br > progress_rate:
                    if sel_media_pivot == "네이버" and sel_device != "통합":
                        actions.append("🟠 비중 조절 필요")
                    else:
                        actions.append("🟠 하향")
                else:
                    actions.append("✅ 적정")
            else:
                burn_list.append(None)
                actions.append("—")
        summary_agg["예산"] = budgets_list
        summary_agg["소진율"] = burn_list
        summary_agg[f"액션({label})"] = actions

        # Total 행 추가 (최상단)
        total_row = summary_agg[["노출수", "클릭수", "비용", "CPC기준비용"]].sum()
        total_row["CTR(%)"] = (total_row["클릭수"] / total_row["노출수"] * 100).round(2) if total_row["노출수"] > 0 else 0
        total_row["CPC"] = round(total_row["CPC기준비용"] / total_row["클릭수"]) if total_row["클릭수"] > 0 else 0
        total_row["CPM"] = round(total_row["CPC기준비용"] / total_row["노출수"] * 1000) if total_row["노출수"] > 0 else 0
        total_budget = sum(budgets_list)
        total_row["예산"] = total_budget
        total_row["소진율"] = total_row["비용"] / total_budget if total_budget > 0 else None
        total_row[f"액션({label})"] = ""
        total_row.name = "Total"
        summary_display = pd.concat([total_row.to_frame().T, summary_agg])[["노출수", "클릭수", "CTR(%)", "CPC", "CPM", "비용", "예산", "소진율", f"액션({label})"]].fillna(0)

        st.dataframe(
            summary_display.style.format({
                "노출수": "{:,.0f}",
                "클릭수": "{:,.0f}",
                "CTR(%)": "{:.2f}%",
                "CPC": "₩{:,.0f}",
                "CPM": "₩{:,.0f}",
                "비용": "₩{:,.0f}",
                "예산": "₩{:,.0f}",
                "소진율": "{:.1%}",
            }, na_rep="—"),
            width="stretch",
        )

        st.divider()

        # ── 캠페인구분별 주차 상세 ──
        for cat, grp in pivot_data.groupby("캠페인구분"):
            st.markdown(f"#### {sel_media_pivot} — {cat}")

            agg = (
                grp.groupby("주차")
                .agg(
                    노출수=("노출수", "sum"),
                    클릭수=("클릭수", "sum"),
                    비용=("비용", "sum"),
                    CPC기준비용=("CPC기준비용", "sum"),
                )
                .sort_index()
            )

            total = agg.sum()
            total.name = "합계"
            agg = pd.concat([agg, total.to_frame().T])

            agg["CTR(%)"] = (agg["클릭수"] / agg["노출수"].replace(0, float("nan")) * 100).round(2)
            agg["CPC"] = (agg["CPC기준비용"] / agg["클릭수"].replace(0, float("nan"))).round(0)
            agg["CPM"] = (agg["CPC기준비용"] / agg["노출수"].replace(0, float("nan")) * 1000).round(0)

            display_df = agg[["노출수", "클릭수", "CTR(%)", "CPC", "CPM", "비용"]].fillna(0)

            st.dataframe(
                display_df.style.format({
                    "노출수": "{:,.0f}",
                    "클릭수": "{:,.0f}",
                    "CTR(%)": "{:.2f}%",
                    "CPC": "₩{:,.0f}",
                    "CPM": "₩{:,.0f}",
                    "비용": "₩{:,.0f}",
                }),
                width="stretch",
            )

            # ── 소진율 vs 진척율 ──
            bkey_budget = f"{ym_key}_{sel_media_pivot}_{cat}"
            full_budget = st.session_state.budgets.get(bkey_budget, 0)
            budget = _get_device_budget(cat, full_budget)
            if budget > 0:
                month_cost = grp["비용"].sum()
                burn_rate = month_cost / budget

                c1, c2, c3 = st.columns(3)
                dev_label = f" ({sel_device})" if sel_media_pivot == "네이버" and sel_device != "통합" else ""
                c1.metric(f"소진율{dev_label}", f"{burn_rate:.1%}")
                c2.metric("진척율", f"{progress_rate:.1%}")
                c3.metric(f"누적 비용{dev_label}", f"{month_cost:,.0f}원")

                if burn_rate < progress_rate:
                    if sel_media_pivot == "네이버":
                        msg = "입찰가 상향 필요"
                    else:
                        msg = "일예산 상향 필요"
                    st.info(
                        f"🔵 **[{sel_media_pivot}{dev_label} — {cat}]** "
                        f"소진율({burn_rate:.1%}) < 진척율({progress_rate:.1%}) → **{msg}**"
                    )
                elif burn_rate > progress_rate:
                    if sel_media_pivot == "네이버" and sel_device != "통합":
                        msg = "비중 조절 필요"
                        st.error(
                            f"🔴 **[{sel_media_pivot}{dev_label} — {cat}]** "
                            f"소진율({burn_rate:.1%}) > 진척율({progress_rate:.1%}) → **{msg}**"
                        )
                    elif sel_media_pivot == "네이버":
                        msg = "입찰가 하향 필요"
                        st.warning(
                            f"🟠 **[{sel_media_pivot} — {cat}]** "
                            f"소진율({burn_rate:.1%}) > 진척율({progress_rate:.1%}) → **{msg}**"
                        )
                    else:
                        msg = "일예산 하향 필요"
                        st.warning(
                            f"🟠 **[{sel_media_pivot} — {cat}]** "
                            f"소진율({burn_rate:.1%}) > 진척율({progress_rate:.1%}) → **{msg}**"
                        )

            st.divider()

elif menu_key == "키워드 Top10":
    has_naver = st.session_state.naver_data is not None
    has_google = st.session_state.google_data is not None

    if not has_naver and not has_google:
        st.info("� 사이드바에서 네이버 또는 구글 SA RAW 데이터를 업로드해 주세요.")
    else:
        naver_rules_json = st.session_state.naver_mapping_rules.to_json()
        google_rules_json = st.session_state.google_mapping_rules.to_json()
        frames = []

        if st.session_state.naver_data is not None:
            ndf, err = prepare_naver_data(st.session_state.naver_data, naver_rules_json)
            if err:
                st.error(f"[네이버] {err}")
            else:
                frames.append(ndf)
        if st.session_state.google_data is not None:
            gdf, err = prepare_google_data(st.session_state.google_data, google_rules_json)
            if err:
                st.error(f"[구글] {err}")
            else:
                frames.append(gdf)

        if not frames:
            st.stop()

        kw_combined = pd.concat(frames, ignore_index=True)

        # 연도/월 선택
        today = datetime.date.today()
        kcol_y, kcol_m, _ = st.columns([1, 1, 4])
        kw_year = kcol_y.selectbox("연도", list(range(today.year - 1, today.year + 2)), index=1, key="kw_year")
        kw_month = kcol_m.selectbox("월", list(range(1, 13)), index=today.month - 1, key="kw_month")
        kw_combined = kw_combined[
            (kw_combined["_date"].dt.year == kw_year)
            & (kw_combined["_date"].dt.month == kw_month)
        ]
        if kw_combined.empty:
            st.warning(f"{kw_year}년 {kw_month}월 데이터가 없습니다.")
            st.stop()

        # 캠페인에 '0.' 포함 & 키워드 != '신한카드'
        kw_combined = kw_combined[
            kw_combined["캠페인"].str.contains("0.", na=False)
            & (kw_combined["키워드"].str.strip() != "신한카드")
        ]
        if kw_combined.empty:
            st.warning("필터 조건에 맞는 키워드 데이터가 없습니다.")
            st.stop()

        week_num = kw_combined["주차"].str.extract(r"(\d+)").astype("Int64")[0]
        kw_combined["_week_num"] = week_num

        def get_top10_intersection(data):
            """노출수 상위 ∩ 클릭수 상위 키워드 중 Top 10 추출."""
            kw_agg = data.groupby("키워드").agg(
                노출수=("노출수", "sum"),
                클릭수=("클릭수", "sum"),
            ).reset_index()

            n_pool = max(30, len(kw_agg))
            top_imp = set(kw_agg.nlargest(n_pool, "노출수")["키워드"])
            top_clk = set(kw_agg.nlargest(n_pool, "클릭수")["키워드"])
            both = top_imp & top_clk

            if not both:
                return kw_agg.nlargest(10, "노출수")

            intersection_df = kw_agg[kw_agg["키워드"].isin(both)].copy()
            intersection_df["_score"] = (
                intersection_df["노출수"].rank(ascending=False)
                + intersection_df["클릭수"].rank(ascending=False)
            )
            return intersection_df.nsmallest(10, "_score")[["키워드", "노출수", "클릭수"]]

        def show_top10_table(title, top10_df):
            """Top 10 테이블 표시."""
            st.markdown(f"#### {title}")
            display = top10_df.reset_index(drop=True)
            display.index = display.index + 1
            display.index.name = "순위"
            st.dataframe(
                display.style.format({"노출수": "{:,.0f}", "클릭수": "{:,.0f}"}),
                width="stretch",
            )

        def show_weekly_table(title, data, keywords):
            """Top 10 키워드의 주차별 노출수·클릭수 테이블."""
            st.markdown(f"#### {title} — 주차별 추이")
            filtered = data[data["키워드"].isin(keywords)]
            if filtered.empty:
                st.caption("데이터 없음")
                return

            # 노출수 피벗 (주차=행, 키워드=열)
            imp_pivot = filtered.pivot_table(
                index="_week_num", columns="키워드", values="노출수",
                aggfunc="sum", fill_value=0,
            )
            imp_pivot.index = [f"W{int(c)}" for c in imp_pivot.index]
            imp_pivot.index.name = "주차"
            col_order = imp_pivot.sum().sort_values(ascending=False).index
            imp_pivot = imp_pivot[col_order]
            grand = imp_pivot.sum()
            grand.name = "합계"
            imp_pivot = pd.concat([imp_pivot, grand.to_frame().T])

            # 클릭수 피벗 (주차=행, 키워드=열)
            clk_pivot = filtered.pivot_table(
                index="_week_num", columns="키워드", values="클릭수",
                aggfunc="sum", fill_value=0,
            )
            clk_pivot.index = [f"W{int(c)}" for c in clk_pivot.index]
            clk_pivot.index.name = "주차"
            clk_pivot = clk_pivot[col_order]
            grand_c = clk_pivot.sum()
            grand_c.name = "합계"
            clk_pivot = pd.concat([clk_pivot, grand_c.to_frame().T])

            st.caption("노출수")
            st.dataframe(
                imp_pivot.style.format("{:,.0f}"),
                width="stretch",
            )
            st.caption("클릭수")
            st.dataframe(
                clk_pivot.style.format("{:,.0f}"),
                width="stretch",
            )

        # ── 매체별 Top 10 ──
        media_top10 = {}
        for media_name in ["네이버", "구글"]:
            media_data = kw_combined[kw_combined["매체"] == media_name]
            if media_data.empty:
                continue
            top10 = get_top10_intersection(media_data)
            media_top10[media_name] = top10
            show_top10_table(f"🔑 {media_name} 키워드 Top 10", top10)

        st.divider()

        # ── 통합 Top 10 ──
        unified = kw_combined.copy()
        unified["키워드"] = unified["키워드"].str.replace(r"^\[|\]$", "", regex=True).str.strip()
        unified_top10 = get_top10_intersection(unified)
        show_top10_table("🔑 통합 키워드 Top 10 (네이버 + 구글)", unified_top10)

        st.divider()

        # ── 주차별 추이 테이블 ──
        st.subheader("📈 Top 10 키워드 주차별 추이")
        unified_kw = kw_combined.copy()
        unified_kw["키워드"] = unified_kw["키워드"].str.replace(r"^\[|\]$", "", regex=True).str.strip()

        for media_name in ["네이버", "구글"]:
            if media_name in media_top10:
                kws = media_top10[media_name]["키워드"].tolist()
                media_data = kw_combined[kw_combined["매체"] == media_name]
                show_weekly_table(f"{media_name} Top 10", media_data, kws)
                st.divider()

        unified_kws = unified_top10["키워드"].tolist()
        show_weekly_table("통합 Top 10", unified_kw, unified_kws)

elif menu_key == "미노출 현황":
    st.subheader("🚫 네이버 키워드 미노출 현황판")

    # ── 키워드 리스트 업로드 ──
    kw_file = st.file_uploader(
        "네이버 전체 운영 키워드 리스트 업로드 (캠페인, 광고그룹, 키워드)",
        type=["csv", "xlsx", "xls"],
        key="exposure_kw_upload",
    )

    if kw_file is not None:
        # 파일 읽기
        try:
            if kw_file.name.endswith(".csv"):
                kw_df = pd.read_csv(kw_file)
            else:
                kw_df = pd.read_excel(kw_file)
        except Exception as e:
            st.error(f"파일 읽기 오류: {e}")
            st.stop()

        # 컬럼명 표준화
        col_map = {}
        for c in kw_df.columns:
            cl = c.strip()
            if "캠페인" in cl and "구분" not in cl:
                col_map[c] = "캠페인"
            elif "광고그룹" in cl or "그룹" in cl:
                col_map[c] = "광고그룹"
            elif "키워드" in cl:
                col_map[c] = "키워드"
        kw_df = kw_df.rename(columns=col_map)

        required_cols = ["캠페인", "광고그룹", "키워드"]
        missing = [c for c in required_cols if c not in kw_df.columns]
        if missing:
            st.error(f"필수 컬럼 누락: {', '.join(missing)}")
            st.stop()

        # 캠페인에 '0.' 포함 데이터만 추출
        kw_df = kw_df[kw_df["캠페인"].astype(str).str.contains("0.", na=False)]

        # 매핑 규칙 적용 → 캠페인구분
        naver_rules_json = st.session_state.naver_mapping_rules.to_json()
        mapping_rules = pd.read_json(io.StringIO(naver_rules_json))
        kw_df = apply_mapping_rules(kw_df, mapping_rules)

        # 디바이스 추출
        kw_df["디바이스"] = map_device_vec(kw_df["캠페인"])

        # 중복 키워드 제거 (캠페인구분 + 디바이스 + 키워드 기준)
        kw_df = kw_df.drop_duplicates(subset=["캠페인구분", "디바이스", "키워드"])

        st.success(f"✅ 총 **{len(kw_df)}**개 키워드 로드 (캠페인에 '0.' 포함)")

        # ── 필터 ──
        fcol1, fcol2 = st.columns(2)
        cats = sorted(kw_df["캠페인구분"].unique())
        sel_cats = fcol1.multiselect("캠페인구분 필터", cats, default=cats, key="exp_cat_filter")
        devices = sorted(kw_df["디바이스"].unique())
        sel_devs = fcol2.multiselect("디바이스 필터", devices, default=devices, key="exp_dev_filter")

        filtered = kw_df[kw_df["캠페인구분"].isin(sel_cats) & kw_df["디바이스"].isin(sel_devs)]
        st.caption(f"필터 적용: **{len(filtered)}**개 키워드")

        # ── 확인 버튼 및 마지막 체크 시각 ──
        btn_col, info_col = st.columns([1, 3])
        do_check = btn_col.button("🔍 노출 여부 확인", type="primary")

        if "exposure_results" not in st.session_state:
            st.session_state.exposure_results = {}
        if "exposure_check_time" not in st.session_state:
            st.session_state.exposure_check_time = None

        if st.session_state.exposure_check_time:
            elapsed = (datetime.datetime.now() - st.session_state.exposure_check_time).total_seconds()
            info_col.caption(
                f"마지막 확인: {st.session_state.exposure_check_time.strftime('%Y-%m-%d %H:%M:%S')} "
                f"({elapsed/60:.0f}분 전)"
            )
            if elapsed >= 3600:
                info_col.info("⏰ 1시간이 경과했습니다. 재확인을 권장합니다.")

        if do_check:
            unique_kws = filtered["키워드"].unique()
            results = {}
            progress = st.progress(0, text="노출 여부 확인 중...")
            status_container = st.empty()

            for i, kw in enumerate(unique_kws):
                status_container.caption(f"확인 중: `{kw}` ({i+1}/{len(unique_kws)})")
                pc_res = check_naver_ad_exposure(kw, "PC")
                time.sleep(0.3)
                mo_res = check_naver_ad_exposure(kw, "MO")
                time.sleep(0.3)
                results[kw] = {"PC": pc_res, "MO": mo_res}
                progress.progress((i + 1) / len(unique_kws))

            progress.empty()
            status_container.empty()
            st.session_state.exposure_results = results
            st.session_state.exposure_check_time = datetime.datetime.now()
            st.toast("✅ 노출 여부 확인 완료")
            st.rerun()

        # ── 결과 표시 ──
        results = st.session_state.exposure_results
        if results:
            filtered = filtered.copy()
            filtered["PC_노출"] = filtered["키워드"].map(lambda k: results.get(k, {}).get("PC", "—"))
            filtered["MO_노출"] = filtered["키워드"].map(lambda k: results.get(k, {}).get("MO", "—"))

            # 전체 요약
            total_kw = len(filtered)
            pc_ok = (filtered["PC_노출"] == "노출").sum()
            mo_ok = (filtered["MO_노출"] == "노출").sum()
            pc_no = (filtered["PC_노출"] == "미노출").sum()
            mo_no = (filtered["MO_노출"] == "미노출").sum()

            mc1, mc2, mc3, mc4 = st.columns(4)
            mc1.metric("PC 노출", f"{pc_ok}개")
            mc2.metric("PC 미노출", f"{pc_no}개")
            mc3.metric("MO 노출", f"{mo_ok}개")
            mc4.metric("MO 미노출", f"{mo_no}개")

            st.divider()

            def _color_exposure(val):
                if val == "노출":
                    return "background-color: #d4edda; color: #155724"
                elif val == "미노출":
                    return "background-color: #f8d7da; color: #721c24"
                return ""

            # 디바이스 · 캠페인구분별 그룹 표시
            for dev in sel_devs:
                for cat in sel_cats:
                    subset = filtered[
                        (filtered["디바이스"] == dev) & (filtered["캠페인구분"] == cat)
                    ]
                    if subset.empty:
                        continue

                    n = len(subset)
                    pc_cnt = (subset["PC_노출"] == "노출").sum()
                    mo_cnt = (subset["MO_노출"] == "노출").sum()

                    st.markdown(
                        f"#### {dev} — {cat}　"
                        f"<small style='color:gray'>"
                        f"PC {pc_cnt}/{n} ({pc_cnt/n:.0%}) · MO {mo_cnt}/{n} ({mo_cnt/n:.0%})"
                        f"</small>",
                        unsafe_allow_html=True,
                    )

                    display = subset[["캠페인", "광고그룹", "키워드", "PC_노출", "MO_노출"]].reset_index(drop=True)
                    display.index += 1
                    display.index.name = "#"

                    st.dataframe(
                        display.style.map(_color_exposure, subset=["PC_노출", "MO_노출"]),
                        width="stretch",
                    )
                    st.divider()
        elif kw_file is not None:
            st.info("🔍 '노출 여부 확인' 버튼을 눌러주세요.")

elif menu_key == "급상승·급하락 키워드":
    st.subheader("📈 주차별 급상승 · 급하락 키워드")

    has_naver = st.session_state.naver_data is not None
    has_google = st.session_state.google_data is not None

    if not has_naver and not has_google:
        st.info("� 사이드바에서 네이버 또는 구글 SA RAW 데이터를 업로드해 주세요.")
    else:
        naver_rules_json = st.session_state.naver_mapping_rules.to_json()
        google_rules_json = st.session_state.google_mapping_rules.to_json()
        frames = []
        if has_naver:
            ndf, err = prepare_naver_data(st.session_state.naver_data, naver_rules_json)
            if not err:
                frames.append(ndf)
        if has_google:
            gdf, err = prepare_google_data(st.session_state.google_data, google_rules_json)
            if not err:
                frames.append(gdf)

        if not frames:
            st.warning("데이터 처리 중 오류가 발생했습니다.")
            st.stop()

        surge_data = pd.concat(frames, ignore_index=True)

        # 구글 키워드 [ ] 제거 (통합 시 네이버와 동일 키워드로 매칭)
        google_mask = surge_data["매체"] == "구글"
        surge_data.loc[google_mask, "키워드"] = (
            surge_data.loc[google_mask, "키워드"].str.replace(r"[\[\]]", "", regex=True).str.strip()
        )

        # 캠페인 필터: '0.' 포함, 키워드 '신한카드' 제외
        surge_data = surge_data[surge_data["캠페인"].str.contains("0.", na=False)]
        surge_data = surge_data[surge_data["키워드"] != "신한카드"]

        # 주차 번호 추출
        surge_data["_wn"] = surge_data["주차"].str.extract(r"(\d+)").astype("Int64")[0]

        # 매체 선택
        available_media = sorted(surge_data["매체"].unique())
        sel_surge_media = st.radio("매체 선택", ["통합"] + available_media, horizontal=True, key="surge_media")

        if sel_surge_media != "통합":
            surge_data = surge_data[surge_data["매체"] == sel_surge_media]

        # ── 연도 / 월 / 주차 선택 ──
        last_date = surge_data["_date"].max()
        last_date_d = last_date.date() if hasattr(last_date, "date") and not pd.isna(last_date) else datetime.date.today()
        avail_years = sorted(surge_data["_date"].dt.year.dropna().unique())
        default_yr_idx = avail_years.index(last_date_d.year) if last_date_d.year in avail_years else len(avail_years) - 1

        sc1, sc2, sc3 = st.columns(3)
        sel_surge_year = sc1.selectbox("연도", avail_years, index=default_yr_idx, key="surge_year")
        avail_months = sorted(
            surge_data[surge_data["_date"].dt.year == sel_surge_year]["_date"].dt.month.dropna().unique()
        )
        default_mo_idx = avail_months.index(last_date_d.month) if last_date_d.month in avail_months else len(avail_months) - 1
        sel_surge_month = sc2.selectbox("월", avail_months, index=default_mo_idx, key="surge_month")

        # 선택 연/월로 필터
        surge_data = surge_data[
            (surge_data["_date"].dt.year == sel_surge_year)
            & (surge_data["_date"].dt.month == sel_surge_month)
        ]
        avail_weeks = sorted(surge_data["_wn"].dropna().unique())
        if len(avail_weeks) == 0:
            st.warning("선택한 연/월에 데이터가 없습니다.")
            st.stop()

        default_wn = (last_date_d.day - 1) // 7 + 1 if last_date_d.month == sel_surge_month and last_date_d.year == sel_surge_year else avail_weeks[-1]
        default_wn_idx = avail_weeks.index(default_wn) if default_wn in avail_weeks else len(avail_weeks) - 1
        sel_surge_wn = sc3.selectbox("이번주 (비교 기준)", [f"W{w}" for w in avail_weeks], index=default_wn_idx, key="surge_wn")
        this_week = int(sel_surge_wn.replace("W", ""))

        # W1 선택 시 전월 마지막 주차를 지난주로 사용
        use_prev_month = this_week == 1
        if use_prev_month:
            if sel_surge_month == 1:
                prev_year, prev_month = sel_surge_year - 1, 12
            else:
                prev_year, prev_month = sel_surge_year, sel_surge_month - 1

            # 전월 데이터 로드 (필터 전 원본 frames 다시 결합)
            prev_all = pd.concat(frames, ignore_index=True)
            prev_google_mask = prev_all["매체"] == "구글"
            prev_all.loc[prev_google_mask, "키워드"] = (
                prev_all.loc[prev_google_mask, "키워드"].str.replace(r"[\[\]]", "", regex=True).str.strip()
            )
            prev_all = prev_all[prev_all["캠페인"].str.contains("0.", na=False)]
            prev_all = prev_all[prev_all["키워드"] != "신한카드"]
            prev_all["_wn"] = prev_all["주차"].str.extract(r"(\d+)").astype("Int64")[0]
            if sel_surge_media != "통합":
                prev_all = prev_all[prev_all["매체"] == sel_surge_media]
            prev_month_data = prev_all[
                (prev_all["_date"].dt.year == prev_year) & (prev_all["_date"].dt.month == prev_month)
            ]
            if prev_month_data.empty:
                st.warning(f"{prev_year}년 {prev_month}월 데이터가 없어 전월 비교가 불가합니다.")
                st.stop()
            last_week = int(prev_month_data["_wn"].max())
            lw_source = prev_month_data
            lw_label = f"{prev_year}년 {prev_month}월 W{last_week}"
        else:
            last_week = this_week - 1
            lw_source = surge_data
            lw_label = f"W{last_week}"

        st.caption(f"📅 비교: {lw_label} → W{this_week} ({sel_surge_year}년 {sel_surge_month}월)")

        # 이번주·지난주 실제 일수 계산 (일평균 보정용)
        tw_days = surge_data[surge_data["_wn"] == this_week]["_date"].dt.date.nunique()
        lw_days = lw_source[lw_source["_wn"] == last_week]["_date"].dt.date.nunique()
        tw_days = max(tw_days, 1)
        lw_days = max(lw_days, 1)

        need_normalize = tw_days != lw_days
        if need_normalize:
            st.info(
                f"ℹ️ 이번주(W{this_week}) {tw_days}일 vs 지난주({lw_label}) {lw_days}일 "
                f"→ 지난주 값을 일평균 × {tw_days}일로 보정하여 비교합니다."
            )

        # 이번주·지난주 키워드별 집계
        tw = surge_data[surge_data["_wn"] == this_week].groupby("키워드").agg(
            이번주_노출수=("노출수", "sum"), 이번주_클릭수=("클릭수", "sum"),
        )
        lw = lw_source[lw_source["_wn"] == last_week].groupby("키워드").agg(
            지난주_노출수=("노출수", "sum"), 지난주_클릭수=("클릭수", "sum"),
        )

        # 일수가 다르면 지난주를 일평균 × 이번주 일수로 보정
        if need_normalize:
            lw["지난주_노출수"] = (lw["지난주_노출수"] / lw_days * tw_days).round(0).astype(int)
            lw["지난주_클릭수"] = (lw["지난주_클릭수"] / lw_days * tw_days).round(0).astype(int)

        merged = tw.join(lw, how="outer").fillna(0).astype(int)

        # 증감율 계산
        merged["노출_증감율(%)"] = ((merged["이번주_노출수"] - merged["지난주_노출수"])
                                    / merged["지난주_노출수"].replace(0, float("nan")) * 100).round(1)
        merged["클릭_증감율(%)"] = ((merged["이번주_클릭수"] - merged["지난주_클릭수"])
                                    / merged["지난주_클릭수"].replace(0, float("nan")) * 100).round(1)

        # ── 스타일 함수 ──
        def _color_change(val):
            if pd.isna(val):
                return ""
            if val > 0:
                return "color: #d32f2f; font-weight: bold"
            elif val < 0:
                return "color: #1565c0; font-weight: bold"
            return ""

        def _show_surge_table(title, df, cols, change_col, emoji_up="🔺", emoji_down="🔻"):
            """급상승/급하락 테이블 출력."""
            display = df[cols].copy()
            display.index.name = "키워드"
            display = display.reset_index()
            display.index += 1
            display.index.name = "#"
            st.dataframe(
                display.style.map(_color_change, subset=[change_col]).format(
                    {c: "{:,.0f}" for c in cols if c != change_col} | {change_col: "{:+.1f}%"}
                ),
                width="stretch",
                height=420,
            )

        # ══════ 노출수 ══════
        st.markdown(f"### 노출수 급상승 · 급하락 (W{last_week} → W{this_week})")
        st.caption("이번주 노출수 ≥ 1,000 키워드 대상")

        imp_target = merged[merged["이번주_노출수"] >= 1000].copy()
        imp_target = imp_target.dropna(subset=["노출_증감율(%)"])
        imp_cols = ["지난주_노출수", "이번주_노출수", "노출_증감율(%)"]

        if imp_target.empty:
            st.info("조건에 해당하는 키워드가 없습니다.")
        else:
            ic1, ic2 = st.columns(2)
            with ic1:
                st.markdown("#### 🔺 급상승 Top 10")
                top10_up = imp_target.nlargest(10, "노출_증감율(%)")
                _show_surge_table("급상승", top10_up, imp_cols, "노출_증감율(%)")
            with ic2:
                st.markdown("#### 🔻 급하락 Top 10")
                top10_down = imp_target.nsmallest(10, "노출_증감율(%)")
                _show_surge_table("급하락", top10_down, imp_cols, "노출_증감율(%)")

        st.divider()

        # ══════ 클릭수 ══════
        st.markdown(f"### 클릭수 급상승 · 급하락 (W{last_week} → W{this_week})")
        st.caption("이번주 클릭수 ≥ 20 키워드 대상")

        clk_target = merged[merged["이번주_클릭수"] >= 20].copy()
        clk_target = clk_target.dropna(subset=["클릭_증감율(%)"])
        clk_cols = ["지난주_클릭수", "이번주_클릭수", "클릭_증감율(%)"]

        if clk_target.empty:
            st.info("조건에 해당하는 키워드가 없습니다.")
        else:
            cc1, cc2 = st.columns(2)
            with cc1:
                st.markdown("#### 🔺 급상승 Top 10")
                clk_up = clk_target.nlargest(10, "클릭_증감율(%)")
                _show_surge_table("급상승", clk_up, clk_cols, "클릭_증감율(%)")
            with cc2:
                st.markdown("#### 🔻 급하락 Top 10")
                clk_down = clk_target.nsmallest(10, "클릭_증감율(%)")
                _show_surge_table("급하락", clk_down, clk_cols, "클릭_증감율(%)")

elif menu_key == "경쟁사 쿼리 추이":
    st.subheader("🏢 경쟁사 쿼리량 트렌드")

    query_file = st.file_uploader(
        "경쟁사 쿼리 데이터 업로드 (날짜, 키워드, PC 검색량, 모바일 검색량)",
        type=["csv", "xlsx", "xls"],
        key="competitor_query_upload",
    )

    if query_file is not None:
        try:
            if query_file.name.endswith(".csv"):
                qdf = pd.read_csv(query_file)
            else:
                qdf = pd.read_excel(query_file)
        except Exception as e:
            st.error(f"파일 읽기 오류: {e}")
            st.stop()

        # 컬럼 표준화
        col_map = {}
        for c in qdf.columns:
            cl = c.strip()
            if "날짜" in cl or "일자" in cl:
                col_map[c] = "날짜"
            elif "키워드" in cl or "카드사" in cl:
                col_map[c] = "카드사"
            elif "PC" in cl and "검색" in cl:
                col_map[c] = "PC"
            elif ("모바일" in cl or "MO" in cl.upper()) and "검색" in cl:
                col_map[c] = "MO"
            elif "총" in cl and "검색" in cl:
                col_map[c] = "총검색량"
        qdf = qdf.rename(columns=col_map)

        if "날짜" not in qdf.columns or "카드사" not in qdf.columns:
            st.error("필수 컬럼(날짜, 키워드/카드사)이 누락되었습니다.")
            st.stop()

        qdf["날짜"] = pd.to_datetime(qdf["날짜"], errors="coerce")
        for col in ["PC", "MO"]:
            if col in qdf.columns:
                qdf[col] = pd.to_numeric(qdf[col], errors="coerce").fillna(0).astype(int)

        # 카드사명 간소화
        qdf["카드사"] = qdf["카드사"].str.replace("카드", "").str.strip()

        last_date = qdf["날짜"].max()
        st.caption(f"📅 데이터 마지막 일자: {last_date.strftime('%Y-%m-%d')}")

        # ── 디바이스 선택 ──
        device_opts = []
        if "PC" in qdf.columns:
            device_opts.append("PC")
        if "MO" in qdf.columns:
            device_opts.append("MO")
        if "PC" in qdf.columns and "MO" in qdf.columns:
            device_opts.insert(0, "통합(PC+MO)")

        sel_device_q = st.radio("디바이스 선택", device_opts, horizontal=True, key="q_device")

        if sel_device_q == "통합(PC+MO)":
            qdf["쿼리량"] = qdf.get("PC", 0) + qdf.get("MO", 0)
        elif sel_device_q == "PC":
            qdf["쿼리량"] = qdf["PC"]
        else:
            qdf["쿼리량"] = qdf["MO"]

        # ── 기간 필터: 최근 30일 vs 7일 전 기준 30일 ──
        end_recent = last_date
        start_recent = end_recent - pd.Timedelta(days=29)
        end_prev = last_date - pd.Timedelta(days=7)
        start_prev = end_prev - pd.Timedelta(days=29)

        recent = qdf[(qdf["날짜"] >= start_recent) & (qdf["날짜"] <= end_recent)]
        prev = qdf[(qdf["날짜"] >= start_prev) & (qdf["날짜"] <= end_prev)]

        # ── 카드사별 합계 & delta metric ──
        recent_agg = recent.groupby("카드사")["쿼리량"].sum()
        prev_agg = prev.groupby("카드사")["쿼리량"].sum()

        card_list = sorted(recent_agg.index)
        n_cards = len(card_list) or 1
        metric_cols = st.columns(n_cards)

        for i, card in enumerate(card_list):
            r_val = int(recent_agg.get(card, 0))
            p_val = int(prev_agg.get(card, 0))
            if p_val > 0:
                delta_pct = (r_val - p_val) / p_val * 100
                delta_str = f"{delta_pct:+.1f}% ({r_val - p_val:+,})"
            else:
                delta_str = "N/A"
            metric_cols[i].metric(
                label=f"{card} ({sel_device_q})",
                value=f"{r_val:,}",
                delta=delta_str,
            )

        st.caption(
            f"최근 30일: {start_recent.strftime('%m/%d')}~{end_recent.strftime('%m/%d')} vs "
            f"이전 30일: {start_prev.strftime('%m/%d')}~{end_prev.strftime('%m/%d')}"
        )

        st.divider()

        # ── 라인 차트 ──
        st.markdown("### 📈 카드사별 쿼리량 추이 (최근 30일)")
        chart_data = recent.groupby(["날짜", "카드사"])["쿼리량"].sum().reset_index()
        chart_pivot = chart_data.pivot(index="날짜", columns="카드사", values="쿼리량").fillna(0)
        chart_pivot = chart_pivot.sort_index()

        st.line_chart(chart_pivot, use_container_width=True)

    else:
        st.info("📤 경쟁사 쿼리 데이터 파일을 업로드해 주세요.")

elif menu_key == "주간 AI 코멘트":
    st.subheader("🤖 주간 AI 운영 코멘트")

    # ── LLM 제공자 선택 ──
    llm_provider = st.radio(
        "AI 제공자 선택",
        ["Google Gemini (무료)", "Groq (무료)", "OpenAI (유료)"],
        horizontal=True,
        key="llm_provider",
    )

    if "OpenAI" in llm_provider:
        api_key = st.text_input("OpenAI API Key", type="password", key="openai_api_key",
                                placeholder="sk-...")
        st.caption("💡 [platform.openai.com](https://platform.openai.com/api-keys)에서 발급")
    elif "Gemini" in llm_provider:
        api_key = st.text_input("Google Gemini API Key", type="password", key="gemini_api_key",
                                placeholder="AIza...")
        st.caption("💡 [aistudio.google.com](https://aistudio.google.com/apikey)에서 무료 발급")
    else:
        api_key = st.text_input("Groq API Key", type="password", key="groq_api_key",
                                placeholder="gsk_...")
        st.caption("💡 [console.groq.com](https://console.groq.com/keys)에서 무료 발급")

    # ── 데이터 요약 함수 ──
    def _summarize_for_ai():
        """1번 메뉴(예산 소진율) + 4번 메뉴(급상승/하락 키워드) 데이터를 텍스트로 요약."""
        lines = []

        has_naver = st.session_state.naver_data is not None
        has_google = st.session_state.google_data is not None
        if not has_naver and not has_google:
            return None

        naver_rules_json = st.session_state.naver_mapping_rules.to_json()
        google_rules_json = st.session_state.google_mapping_rules.to_json()
        frames = []
        if has_naver:
            ndf, err = prepare_naver_data(st.session_state.naver_data, naver_rules_json)
            if not err:
                frames.append(ndf)
        if has_google:
            gdf, err = prepare_google_data(st.session_state.google_data, google_rules_json)
            if not err:
                frames.append(gdf)
        if not frames:
            return None

        combined = pd.concat(frames, ignore_index=True)
        if combined["_date"].dropna().empty:
            return None
        last_dt = combined["_date"].max()
        ref_year, ref_month = last_dt.year, last_dt.month
        days_in_month = calendar.monthrange(ref_year, ref_month)[1]
        today = datetime.date.today()
        if today.year == ref_year and today.month == ref_month:
            progress_rate = (today.day - 1) / days_in_month
        else:
            progress_rate = 1.0
        ym_key = f"{ref_year}-{ref_month:02d}"
        month_filter = (combined["_date"].dt.year == ref_year) & (combined["_date"].dt.month == ref_month)
        combined = combined[month_filter]

        if combined.empty:
            return None

        # ── 매체/캠페인별 예산 소진율 ──
        lines.append("## 매체/캠페인별 예산 소진율")
        lines.append(f"기준월: {ym_key} | 진척율: {progress_rate:.1%}")
        for media in sorted(combined["매체"].unique()):
            mdata = combined[combined["매체"] == media]
            for cat in sorted(mdata["캠페인구분"].unique()):
                if cat == "기타":
                    continue
                cdata = mdata[mdata["캠페인구분"] == cat]
                cost = cdata["비용"].sum()
                bk = f"{ym_key}_{media}_{cat}"
                budget = st.session_state.budgets.get(bk, 0)
                if budget > 0:
                    burn = cost / budget
                    status = "상향 필요" if burn < progress_rate else ("하향 필요" if burn > progress_rate else "적정")
                    lines.append(f"- {media} {cat}: 예산 {budget:,.0f}원, 비용 {cost:,.0f}원, 소진율 {burn:.1%} → {status}")
                else:
                    lines.append(f"- {media} {cat}: 예산 미입력, 비용 {cost:,.0f}원")

        # ── 급상승/하락 키워드 ──
        lines.append("")
        lines.append("## 급상승/급하락 키워드")

        surge_all = pd.concat(frames, ignore_index=True)
        google_mask = surge_all["매체"] == "구글"
        surge_all.loc[google_mask, "키워드"] = (
            surge_all.loc[google_mask, "키워드"].str.replace(r"[\[\]]", "", regex=True).str.strip()
        )
        surge_all = surge_all[surge_all["캠페인"].str.contains("0.", na=False)]
        surge_all = surge_all[surge_all["키워드"] != "신한카드"]
        surge_all = surge_all[(surge_all["_date"].dt.year == ref_year) & (surge_all["_date"].dt.month == ref_month)]
        surge_all["_wn"] = surge_all["주차"].str.extract(r"(\d+)").astype("Int64")[0]

        last_date = surge_all["_date"].max()
        if pd.notna(last_date):
            current_wn = (last_date.day - 1) // 7 + 1 if hasattr(last_date, "day") else 1
            this_wk = current_wn
            last_wk = current_wn - 1

            if last_wk >= 1:
                tw = surge_all[surge_all["_wn"] == this_wk].groupby("키워드").agg(
                    이번주_노출수=("노출수", "sum"), 이번주_클릭수=("클릭수", "sum"))
                lw = surge_all[surge_all["_wn"] == last_wk].groupby("키워드").agg(
                    지난주_노출수=("노출수", "sum"), 지난주_클릭수=("클릭수", "sum"))

                tw_days = max(surge_all[surge_all["_wn"] == this_wk]["_date"].dt.date.nunique(), 1)
                lw_days = max(surge_all[surge_all["_wn"] == last_wk]["_date"].dt.date.nunique(), 1)
                if tw_days < lw_days:
                    lw["지난주_노출수"] = (lw["지난주_노출수"] / lw_days * tw_days).round(0).astype(int)
                    lw["지난주_클릭수"] = (lw["지난주_클릭수"] / lw_days * tw_days).round(0).astype(int)

                merged = tw.join(lw, how="outer").fillna(0).astype(int)
                merged["노출_증감율"] = ((merged["이번주_노출수"] - merged["지난주_노출수"])
                                          / merged["지난주_노출수"].replace(0, float("nan")) * 100).round(1)
                merged["클릭_증감율"] = ((merged["이번주_클릭수"] - merged["지난주_클릭수"])
                                          / merged["지난주_클릭수"].replace(0, float("nan")) * 100).round(1)

                imp_t = merged[merged["이번주_노출수"] >= 1000].dropna(subset=["노출_증감율"])
                if not imp_t.empty:
                    lines.append(f"\n노출수 급상승 Top5 (W{last_wk}→W{this_wk}):")
                    for kw, row in imp_t.nlargest(5, "노출_증감율").iterrows():
                        lines.append(f"  - {kw}: {row['지난주_노출수']:,.0f}→{row['이번주_노출수']:,.0f} ({row['노출_증감율']:+.1f}%)")
                    lines.append(f"노출수 급하락 Top5:")
                    for kw, row in imp_t.nsmallest(5, "노출_증감율").iterrows():
                        lines.append(f"  - {kw}: {row['지난주_노출수']:,.0f}→{row['이번주_노출수']:,.0f} ({row['노출_증감율']:+.1f}%)")

                clk_t = merged[merged["이번주_클릭수"] >= 20].dropna(subset=["클릭_증감율"])
                if not clk_t.empty:
                    lines.append(f"\n클릭수 급상승 Top5:")
                    for kw, row in clk_t.nlargest(5, "클릭_증감율").iterrows():
                        lines.append(f"  - {kw}: {row['지난주_클릭수']:,.0f}→{row['이번주_클릭수']:,.0f} ({row['클릭_증감율']:+.1f}%)")
                    lines.append(f"클릭수 급하락 Top5:")
                    for kw, row in clk_t.nsmallest(5, "클릭_증감율").iterrows():
                        lines.append(f"  - {kw}: {row['지난주_클릭수']:,.0f}→{row['이번주_클릭수']:,.0f} ({row['클릭_증감율']:+.1f}%)")

        return "\n".join(lines)

    # ── 요약 미리보기 ──
    summary_text = _summarize_for_ai()
    if summary_text:
        with st.expander("📋 AI에 전달될 데이터 요약 (미리보기)", expanded=False):
            st.text(summary_text)
    else:
        st.warning("📤 사이드바에서 SA RAW 데이터를 먼저 업로드해 주세요.")

    # ── 코멘트 생성 ──
    generate = st.button("🤖 코멘트 생성", type="primary", disabled=not api_key or not summary_text)

    if generate and api_key and summary_text:
        system_prompt = (
            "당신은 신한카드 SA 퍼포먼스 마케터입니다. "
            "주어진 데이터를 분석해 이번 주 예산 이관, 비효율 키워드 관리 등 "
            "운영 전략 코멘트를 3가지 불릿 포인트로 작성해 주세요."
        )
        with st.spinner("AI 코멘트 생성 중..."):
            try:
                comment = None

                if "OpenAI" in llm_provider:
                    client = OpenAI(api_key=api_key)
                    response = client.chat.completions.create(
                        model="gpt-3.5-turbo",
                        messages=[
                            {"role": "system", "content": system_prompt},
                            {"role": "user", "content": summary_text},
                        ],
                        temperature=0.7,
                        max_tokens=1024,
                    )
                    comment = response.choices[0].message.content

                elif "Gemini" in llm_provider:
                    from google import genai
                    gemini_client = genai.Client(api_key=api_key)
                    response = gemini_client.models.generate_content(
                        model="gemini-2.0-flash-lite",
                        contents=f"{system_prompt}\n\n{summary_text}",
                    )
                    comment = response.text

                elif "Groq" in llm_provider:
                    from groq import Groq
                    groq_client = Groq(api_key=api_key)
                    response = groq_client.chat.completions.create(
                        model="llama-3.3-70b-versatile",
                        messages=[
                            {"role": "system", "content": system_prompt},
                            {"role": "user", "content": summary_text},
                        ],
                        temperature=0.7,
                        max_tokens=1024,
                    )
                    comment = response.choices[0].message.content

                if comment:
                    st.divider()
                    st.markdown("### 📝 AI 운영 코멘트")
                    st.markdown(comment)
            except Exception as e:
                st.error(f"API 호출 실패: {e}")

elif menu_key == "미디어믹스 제안":
    st.subheader("💡 차월 미디어믹스 제안")

    has_naver = st.session_state.naver_data is not None
    has_google = st.session_state.google_data is not None

    if not has_naver and not has_google:
        st.info("� 사이드바에서 네이버 또는 구글 SA RAW 데이터를 업로드해 주세요.")
    else:
        naver_rules_json = st.session_state.naver_mapping_rules.to_json()
        google_rules_json = st.session_state.google_mapping_rules.to_json()
        frames = []
        if has_naver:
            ndf, err = prepare_naver_data(st.session_state.naver_data, naver_rules_json)
            if not err:
                frames.append(ndf)
        if has_google:
            gdf, err = prepare_google_data(st.session_state.google_data, google_rules_json)
            if not err:
                frames.append(gdf)

        if not frames:
            st.warning("데이터 처리 중 오류가 발생했습니다.")
            st.stop()

        mix_data = pd.concat(frames, ignore_index=True)

        # 최근 1개월: 데이터 마지막 일자 기준 30일
        last_dt = mix_data["_date"].max()
        start_dt = last_dt - pd.Timedelta(days=29)
        mix_data = mix_data[(mix_data["_date"] >= start_dt) & (mix_data["_date"] <= last_dt)]
        mix_data = mix_data[mix_data["캠페인구분"] != "기타"]

        st.caption(f"📅 최근 1개월 데이터: {start_dt.strftime('%Y-%m-%d')} ~ {last_dt.strftime('%Y-%m-%d')}")

        if mix_data.empty:
            st.warning("최근 1개월 데이터가 없습니다.")
            st.stop()

        # ── 매체/캠페인구분별 집계 ──
        agg = mix_data.groupby(["매체", "캠페인구분"]).agg(
            노출수=("노출수", "sum"),
            클릭수=("클릭수", "sum"),
            비용=("비용", "sum"),
            CPC기준비용=("CPC기준비용", "sum"),
        ).reset_index()

        agg["평균CPC"] = (agg["CPC기준비용"] / agg["클릭수"].replace(0, float("nan"))).round(0)
        agg["평균CTR"] = (agg["클릭수"] / agg["노출수"].replace(0, float("nan")) * 100).round(2)
        agg["현재비중"] = agg["비용"] / agg["비용"].sum()

        st.markdown("### 📊 최근 1개월 매체/캠페인구분별 성과")
        perf_display = agg[["매체", "캠페인구분", "노출수", "클릭수", "비용", "평균CPC", "평균CTR"]].copy()
        st.dataframe(
            perf_display.style.format({
                "노출수": "{:,.0f}", "클릭수": "{:,.0f}", "비용": "₩{:,.0f}",
                "평균CPC": "₩{:,.0f}", "평균CTR": "{:.2f}%",
            }),
            width="stretch", hide_index=True,
        )

        st.divider()

        # ── 매체별 탭 ──
        media_list = sorted(agg["매체"].unique())
        tabs = st.tabs([f"📋 {m}" for m in media_list])

        for tab, media in zip(tabs, media_list):
            with tab:
                m_agg = agg[agg["매체"] == media].copy()
                existing_cats = sorted(m_agg["캠페인구분"].tolist())

                # ── 캠페인구분 선택/제외 ──
                st.markdown(f"#### {media} — 캠페인구분 관리")
                selected_cats = st.multiselect(
                    "포함할 캠페인구분", existing_cats, default=existing_cats,
                    key=f"mix_cats_{media}",
                )

                # ── 새 캠페인구분 추가 ──
                with st.expander("➕ 새 캠페인구분 추가", expanded=False):
                    ac1, ac2, ac3, ac4 = st.columns(4)
                    new_cat_name = ac1.text_input("구분명", key=f"new_cat_name_{media}", placeholder="예: 신규상품")
                    new_cat_budget = ac2.number_input("예산 (원)", min_value=0, value=0, step=100000, key=f"new_cat_budget_{media}")
                    new_cat_cpc = ac3.number_input("예상 CPC (원)", min_value=0, value=500, step=10, key=f"new_cat_cpc_{media}")
                    new_cat_ctr = ac4.number_input("예상 CTR (%)", min_value=0.0, value=3.0, step=0.1, key=f"new_cat_ctr_{media}")
                    add_btn = st.button("추가", key=f"add_cat_btn_{media}")

                    if f"custom_cats_{media}" not in st.session_state:
                        st.session_state[f"custom_cats_{media}"] = []

                    if add_btn and new_cat_name:
                        st.session_state[f"custom_cats_{media}"].append({
                            "매체": media, "캠페인구분": new_cat_name,
                            "제안예산": new_cat_budget, "예상CPC": new_cat_cpc,
                            "예상CTR(%)": new_cat_ctr,
                        })
                        st.rerun()

                # 기존 데이터 필터
                m_agg = m_agg[m_agg["캠페인구분"].isin(selected_cats)]

                if m_agg.empty and not st.session_state.get(f"custom_cats_{media}", []):
                    st.info("선택된 캠페인구분이 없습니다.")
                    continue

                # ── 예산 입력 & 전략 ──
                st.markdown(f"#### {media} — 차월 예산 배분")
                mc1, mc2 = st.columns(2)
                m_total = mc1.number_input(
                    f"{media} 총예산 (원)", min_value=0,
                    value=int(m_agg["비용"].sum()) if not m_agg.empty else 0,
                    step=1000000, format="%d", key=f"mix_budget_{media}",
                )
                m_strategy = mc2.radio(
                    "배분 전략", ["현재 비중 유지", "효율 최적화 (CPC 중심)"],
                    key=f"mix_strategy_{media}",
                )

                # ── 기존 캠페인구분 예산 배분 ──
                if not m_agg.empty:
                    m_agg["현재비중"] = m_agg["비용"] / m_agg["비용"].sum()
                    proposal = m_agg[["매체", "캠페인구분", "현재비중", "평균CPC", "평균CTR"]].copy()

                    # custom 캠페인들의 예산 합계를 제외한 나머지를 기존에 배분
                    custom_rows = st.session_state.get(f"custom_cats_{media}", [])
                    custom_budget_sum = sum(r["제안예산"] for r in custom_rows)
                    alloc_budget = max(m_total - custom_budget_sum, 0)

                    if m_strategy == "현재 비중 유지":
                        proposal["제안예산"] = (proposal["현재비중"] * alloc_budget).round(0).astype(int)
                    else:
                        cpc_vals = proposal["평균CPC"].fillna(proposal["평균CPC"].max())
                        cpc_inv = 1 / cpc_vals.replace(0, float("nan"))
                        cpc_weight = cpc_inv / cpc_inv.sum()
                        blended = proposal["현재비중"] * 0.9 + cpc_weight * 0.1
                        blended = blended / blended.sum()
                        proposal["제안예산"] = (blended * alloc_budget).round(0).astype(int)

                    diff = alloc_budget - proposal["제안예산"].sum()
                    if len(proposal) > 0:
                        proposal.iloc[-1, proposal.columns.get_loc("제안예산")] += int(diff)

                    proposal["예상CPC"] = proposal["평균CPC"]
                    proposal["예상CTR(%)"] = proposal["평균CTR"]
                else:
                    proposal = pd.DataFrame(columns=["매체", "캠페인구분", "제안예산", "예상CPC", "예상CTR(%)"])

                # ── custom 캠페인 합치기 ──
                custom_rows = st.session_state.get(f"custom_cats_{media}", [])
                if custom_rows:
                    custom_df = pd.DataFrame(custom_rows)[["매체", "캠페인구분", "제안예산", "예상CPC", "예상CTR(%)"]]
                    proposal = pd.concat([
                        proposal[["매체", "캠페인구분", "제안예산", "예상CPC", "예상CTR(%)"]],
                        custom_df
                    ], ignore_index=True)

                proposal["예상클릭수"] = (
                    proposal["제안예산"] / proposal["예상CPC"].replace(0, float("nan"))
                ).round(0).fillna(0).astype(int)

                # ── data_editor ──
                st.markdown(f"#### ✏️ {media} 미디어믹스 (제안예산 수정 가능)")
                editor_df = proposal[["캠페인구분", "제안예산", "예상CPC", "예상CTR(%)", "예상클릭수"]].copy()

                edited = st.data_editor(
                    editor_df,
                    column_config={
                        "캠페인구분": st.column_config.TextColumn("캠페인구분", disabled=True),
                        "제안예산": st.column_config.NumberColumn("제안예산", format="₩%d", min_value=0),
                        "예상CPC": st.column_config.NumberColumn("예상CPC", format="₩%d"),
                        "예상CTR(%)": st.column_config.NumberColumn("예상CTR(%)", format="%.2f%%"),
                        "예상클릭수": st.column_config.NumberColumn("예상클릭수", format="%d", disabled=True),
                    },
                    hide_index=True, use_container_width=True,
                    key=f"mix_editor_{media}",
                )

                edited["예상클릭수"] = (
                    edited["제안예산"] / edited["예상CPC"].replace(0, float("nan"))
                ).round(0).fillna(0).astype(int)

                # ── 매체 요약 ──
                st.divider()
                sc1, sc2, sc3 = st.columns(3)
                sc1.metric(f"{media} 총 제안예산", f"₩{edited['제안예산'].sum():,.0f}")
                sc2.metric("총 예상클릭수", f"{edited['예상클릭수'].sum():,}")
                m_avg_cpc = edited["제안예산"].sum() / max(edited["예상클릭수"].sum(), 1)
                sc3.metric("평균 예상CPC", f"₩{m_avg_cpc:,.0f}")
