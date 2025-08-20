# app_kpi.py
# Streamlit KPI 카드형 대시보드 + 필터 + 업로드/삭제 (MySQL RDS)
# ---------------------------------------------------------------
# Secrets(.streamlit/secrets.toml 또는 Cloud Secrets) 예시:
# [db]                               # 또는 [DB] 로 올려도 됨
# HOST = "my-db-7.c7s06yiach58.ap-northeast-2.rds.amazonaws.com"  # DB_HOST
# PORT = 3306                                                             # DB_PORT
# USER = "admin"                                                          # DB_USER
# PASSWORD = "qwer4321!!K"                                               # DB_PASSWORD
# NAME = "mydata"                                                         # DB_NAME
#
# requirements.txt (핵심)
# streamlit>=1.36
# pandas>=2.2
# SQLAlchemy>=2.0
# pymysql>=1.1
# altair>=5.3
# python-dateutil>=2.9
# cryptography>=42.0
# ---------------------------------------------------------------

import os
import pandas as pd
import altair as alt
import streamlit as st
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text

st.set_page_config(page_title="장애 현황 대시보드", page_icon="📊", layout="wide")
st.title("📊 장애 현황 대시보드")
st.caption("KPI 카드 · 필터 · 업로드/삭제 관리")

# ---------------------------
# DB 연결 (Secrets 읽기 + SSL 강제)
# ---------------------------
@st.cache_resource(show_spinner=False)
def get_engine():
    s = st.secrets
    cfg = {}
    if "db" in s:
        cfg = s["db"]
        host = cfg.get("HOST") or cfg.get("host")
        port = int(cfg.get("PORT") or cfg.get("port") or 3306)
        user = cfg.get("USER") or cfg.get("user")
        pw   = cfg.get("PASSWORD") or cfg.get("password")
        name = cfg.get("NAME") or cfg.get("name")
    elif "DB" in s:
        cfg = s["DB"]
        host = cfg.get("DB_HOST") or cfg.get("HOST")
        port = int(cfg.get("DB_PORT") or cfg.get("PORT") or 3306)
        user = cfg.get("DB_USER") or cfg.get("USER")
        pw   = cfg.get("DB_PASSWORD") or cfg.get("PASSWORD")
        name = cfg.get("DB_NAME") or cfg.get("NAME")
    else:
        host = os.getenv("DB_HOST"); port = int(os.getenv("DB_PORT") or 3306)
        user = os.getenv("DB_USER"); pw = os.getenv("DB_PASSWORD"); name = os.getenv("DB_NAME")

    if not all([host, user, pw, name]):
        st.error("DB secrets가 없습니다. [db] 또는 [DB] 섹션으로 HOST/PORT/USER/PASSWORD/NAME를 등록하세요.")
        st.stop()

    # SQLAlchemy URL
    url = f"mysql+pymysql://{user}:{pw}@{host}:{port}/{name}?charset=utf8mb4"

    # 핵심: MySQL 8 (caching_sha2_password)에서 TLS 없으면 인증 실패 → ssl 강제
    connect_args = {"ssl": {"ssl": True}}

    try:
        eng = create_engine(url, pool_pre_ping=True, connect_args=connect_args)
        # 미리 연결 확인
        with eng.connect() as conn:
            conn.execute(text("SELECT 1"))
        return eng
    except Exception as e:
        st.error(f"DB 연결 실패: {e}")
        st.stop()

engine = get_engine()

# ---------------------------
# 유틸 / 쿼리
# ---------------------------
@st.cache_data(ttl=180, show_spinner=False)
def get_distinct_values():
    with engine.connect() as conn:
        platforms = pd.read_sql(text(
            "SELECT DISTINCT platform FROM incidents "
            "WHERE platform IS NOT NULL AND platform<>'' ORDER BY platform"
        ), conn)["platform"].tolist()
        locales = pd.read_sql(text(
            "SELECT DISTINCT locale FROM incidents "
            "WHERE locale IS NOT NULL AND locale<>'' ORDER BY locale"
        ), conn)["locale"].tolist()
        cats = pd.read_sql(text(
            "SELECT DISTINCT category FROM incidents "
            "WHERE category IS NOT NULL AND category<>'' ORDER BY category"
        ), conn)["category"].tolist()
    return platforms, locales, cats

PLATFORMS, LOCALES, CATEGORIES = get_distinct_values()

# ---------------------------
# 사이드바 필터
# ---------------------------
with st.sidebar:
    st.header("필터")
    today = datetime.now().date()
    default_from = today - timedelta(days=30)
    date_from, date_to = st.date_input("기간(started_at)", value=(default_from, today))
    if isinstance(date_from, tuple):  # 안전장치
        date_from, date_to = date_from

    sel_platforms = st.multiselect("플랫폼", options=PLATFORMS)
    sel_locales   = st.multiselect("로케일", options=LOCALES)
    sel_categories= st.multiselect("카테고리", options=CATEGORIES)
    keyword = st.text_input("키워드(내용/원인/대응/비고)")
    limit = st.number_input("목록 행수", min_value=50, max_value=5000, value=500, step=50)

# WHERE 구성
params = {"date_from": datetime.combine(date_from, datetime.min.time()),
          "date_to":   datetime.combine(date_to,   datetime.max.time())}
where = ["i.started_at BETWEEN :date_from AND :date_to"]
if sel_platforms:
    where.append("i.platform IN :platforms");   params["platforms"]  = tuple(sel_platforms)
if sel_locales:
    where.append("i.locale IN :locales");       params["locales"]    = tuple(sel_locales)
if sel_categories:
    where.append("i.category IN :categories");  params["categories"] = tuple(sel_categories)
if keyword.strip():
    where.append("(i.description LIKE :kw OR i.cause LIKE :kw OR i.response LIKE :kw OR i.note LIKE :kw)")
    params["kw"] = f"%{keyword.strip()}%"
where_sql = " AND ".join(where)

@st.cache_data(ttl=90, show_spinner=False)
def fetch_kpis(where_sql: str, params: dict):
    with engine.connect() as conn:
        total = pd.read_sql(text(f"SELECT COUNT(*) cnt FROM incidents i WHERE {where_sql}"), conn, params=params)["cnt"].iloc[0]
        tparams = dict(params)
        tparams["date_from"] = datetime.combine(datetime.now().date(), datetime.min.time())
        tparams["date_to"]   = datetime.combine(datetime.now().date(), datetime.max.time())
        today_cnt = pd.read_sql(text(f"SELECT COUNT(*) cnt FROM incidents i WHERE {where_sql}"), conn, params=tparams)["cnt"].iloc[0]
        top_cat_df = pd.read_sql(text(
            f"SELECT i.category, COUNT(*) cnt FROM incidents i WHERE {where_sql} GROUP BY i.category ORDER BY cnt DESC LIMIT 1"
        ), conn, params=params)
        top_cat = (top_cat_df["category"].iloc[0], int(top_cat_df["cnt"].iloc[0])) if not top_cat_df.empty else ("-", 0)
        plat_df = pd.read_sql(text(
            f"SELECT i.platform, COUNT(*) cnt FROM incidents i WHERE {where_sql} GROUP BY i.platform ORDER BY cnt DESC"
        ), conn, params=params)
    return total, today_cnt, top_cat, plat_df

@st.cache_data(ttl=90, show_spinner=False)
def fetch_timeseries(where_sql: str, params: dict) -> pd.DataFrame:
    with engine.connect() as conn:
        return pd.read_sql(text(
            f"SELECT DATE(i.started_at) d, COUNT(*) cnt FROM incidents i WHERE {where_sql} GROUP BY DATE(i.started_at) ORDER BY d"
        ), conn, params=params)

@st.cache_data(ttl=90, show_spinner=False)
def fetch_list(where_sql: str, params: dict, limit: int) -> pd.DataFrame:
    q = text(f"""
        SELECT i.id, i.started_at, i.ended_at, i.duration, i.platform, i.locale, i.inquiry_count,
               i.category, i.description, i.cause, i.response, i.note, i.created_at, i.updated_at
        FROM incidents i
        WHERE {where_sql}
        ORDER BY i.started_at DESC
        LIMIT :limit
    """)
    p = dict(params); p["limit"] = int(limit)
    with engine.connect() as conn:
        df = pd.read_sql(q, conn, params=p)
    if not df.empty:
        df["started_at"] = pd.to_datetime(df["started_at"]).dt.strftime("%Y-%m-%d %H:%M")
        df["ended_at"]   = df["ended_at"].apply(lambda x: "" if pd.isna(x) else pd.to_datetime(x).strftime("%Y-%m-%d %H:%M"))
    return df

# ---------------------------
# KPI 카드
# ---------------------------
try:
    total, today_cnt, (top_cat_name, top_cat_cnt), plat_df = fetch_kpis(where_sql, params)
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("총 건수", f"{total:,}")
    c2.metric("오늘 건수", f"{today_cnt:,}")
    c3.metric("최다 카테고리", top_cat_name, delta=f"{top_cat_cnt:,}건")
    with c4:
        st.write("플랫폼별")
        if not plat_df.empty:
            st.altair_chart(
                alt.Chart(plat_df).mark_bar().encode(
                    x=alt.X('platform:N', sort='-y'), y='cnt:Q', tooltip=['platform','cnt']
                ),
                use_container_width=True
            )
        else:
            st.info("데이터 없음")
except Exception as e:
    st.warning(f"KPI 로딩 오류: {e}")

# ---------------------------
# 일별 추이
# ---------------------------
st.subheader("📈 일별 발생 추이")
ts_df = fetch_timeseries(where_sql, params)
if ts_df.empty:
    st.info("데이터가 없습니다.")
else:
    st.altair_chart(
        alt.Chart(ts_df).mark_line(point=True).encode(x='d:T', y='cnt:Q', tooltip=['d:T','cnt:Q']),
        use_container_width=True
    )

# ---------------------------
# 목록
# ---------------------------
st.subheader("📄 사건 목록")
list_df = fetch_list(where_sql, params, int(limit))
if list_df.empty:
    st.info("조건에 맞는 데이터가 없습니다.")
else:
    st.dataframe(list_df, use_container_width=True, height=420)

# ---------------------------
# 관리: 선택 삭제
# ---------------------------
with st.expander("🗑 관리: ID로 선택 삭제"):
    ids = st.text_input("삭제할 ID들(쉼표로 구분)", placeholder="예: 101,102,120")
    if st.button("삭제 실행", type="primary"):
        try:
            id_list = [int(x.strip()) for x in ids.split(',') if x.strip()]
            if not id_list:
                st.warning("ID를 입력하세요.")
            else:
                with engine.begin() as conn:
                    conn.execute(text("DELETE FROM incidents WHERE id IN :ids"), {"ids": tuple(id_list)})
                st.success(f"삭제 완료: {len(id_list)}건")
                st.cache_data.clear()
        except Exception as e:
            st.error(f"삭제 중 오류: {e}")

# ---------------------------
# 관리: 업로드 (CSV/엑셀)
# ---------------------------
with st.expander("⬆️ 관리: 업로드 (CSV/XLSX)"):
    st.caption("가능한 컬럼: started_at, ended_at, duration, platform, locale, inquiry_count, category, description, cause, response, note")
    file = st.file_uploader("파일 선택", type=["csv", "xlsx", "xls"])

    def normalize_df(df: pd.DataFrame) -> pd.DataFrame:
        # 컬럼 정규화
        df.columns = [str(c).strip().lower() for c in df.columns]
        # 날짜 파싱
        for dt in ["started_at", "ended_at"]:
            if dt in df.columns:
                df[dt] = pd.to_datetime(df[dt], errors="coerce")
        if "inquiry_count" in df.columns:
            df["inquiry_count"] = pd.to_numeric(df["inquiry_count"], errors="coerce")
        # 누락 채우기
        for col in ["ended_at","duration","platform","locale","inquiry_count","cause","response","note"]:
            if col not in df.columns:
                df[col] = None
        # 필수 확인
        required = ["started_at", "category", "description"]
        missing = [c for c in required if c not in df.columns]
        if missing:
            raise ValueError(f"필수 컬럼 누락: {missing}")
        return df[["started_at","ended_at","duration","platform","locale","inquiry_count",
                   "category","description","cause","response","note"]]

    if file is not None:
        try:
            up = pd.read_csv(file) if file.name.lower().endswith(".csv") else pd.read_excel(file)
            up = normalize_df(up)
            with engine.begin() as conn:
                up.to_sql("incidents", conn, if_exists="append", index=False)
            st.success(f"업로드 완료: {len(up)}건")
            st.cache_data.clear()
        except Exception as e:
            st.error(f"업로드 실패: {e}")
