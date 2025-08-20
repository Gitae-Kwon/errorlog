# app_kpi.py
# Streamlit KPI 카드형 대시보드 + 필터 + 관리(업로드/삭제)
# -------------------------------------------------------------
# 1) .streamlit/secrets.toml
# [db]
# HOST = "my-db-7.c7s06yiach58.ap-northeast-2.rds.amazonaws.com"
# PORT = 3306
# USER = "admin"
# PASSWORD = "qwer4321!!K"
# NAME = "mydata"
# -------------------------------------------------------------

import pandas as pd
import altair as alt
import streamlit as st
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text

st.set_page_config(page_title="Service Failures Dashboard", page_icon="📊", layout="wide")
st.title("📊 장애 현황 대시보드")
st.caption("KPI 카드 + 필터 + 업로드/삭제 관리")

# ---------------------------
# DB 연결
# ---------------------------
@st.cache_resource(show_spinner=False)
def get_engine():
    cfg = st.secrets["db"]
    url = (
        f"mysql+pymysql://{cfg['USER']}:{cfg['PASSWORD']}@{cfg['HOST']}:{int(cfg['PORT'])}/{cfg['NAME']}?charset=utf8mb4"
    )
    return create_engine(url, pool_pre_ping=True)

engine = get_engine()

# ---------------------------
# 공통 쿼리
# ---------------------------
@st.cache_data(ttl=120, show_spinner=False)
def get_distinct_values():
    sql = text("""
        SELECT DISTINCT platform FROM incidents WHERE platform IS NOT NULL AND platform<>'' ORDER BY platform;
        SELECT DISTINCT locale   FROM incidents WHERE locale   IS NOT NULL AND locale<>''   ORDER BY locale;
        SELECT DISTINCT category FROM incidents WHERE category IS NOT NULL AND category<>'' ORDER BY category;
    """)
    # MySQL은 멀티쿼리를 한번에 못 읽을 수 있어 순차로 실행
    with engine.connect() as conn:
        platforms = pd.read_sql(text("SELECT DISTINCT platform FROM incidents WHERE platform IS NOT NULL AND platform<>'' ORDER BY platform"), conn)["platform"].tolist()
        locales   = pd.read_sql(text("SELECT DISTINCT locale   FROM incidents WHERE locale   IS NOT NULL AND locale<>''   ORDER BY locale"), conn)["locale"].tolist()
        cats      = pd.read_sql(text("SELECT DISTINCT category FROM incidents WHERE category IS NOT NULL AND category<>'' ORDER BY category"), conn)["category"].tolist()
    return platforms, locales, cats

PLATFORMS, LOCALES, CATEGORIES = get_distinct_values()

# ---------------------------
# 사이드바 필터
# ---------------------------
with st.sidebar:
    st.header("필터")
    today = datetime.now().date()
    default_from = today - timedelta(days=30)
    date_from, date_to = st.date_input("기간(started_at 기준)", value=(default_from, today))
    if isinstance(date_from, tuple):
        date_from, date_to = date_from

    sel_platforms = st.multiselect("플랫폼", options=PLATFORMS)
    sel_locales   = st.multiselect("로케일", options=LOCALES)
    sel_categories= st.multiselect("카테고리", options=CATEGORIES)
    keyword = st.text_input("키워드(내용/원인/대응/비고)")
    limit = st.number_input("목록 행수", min_value=50, max_value=5000, value=500, step=50)

params = {
    "date_from": datetime.combine(date_from, datetime.min.time()),
    "date_to":   datetime.combine(date_to,   datetime.max.time()),
}
where = ["i.started_at BETWEEN :date_from AND :date_to"]
if sel_platforms:
    where.append("i.platform IN :platforms")
    params["platforms"] = tuple(sel_platforms)
if sel_locales:
    where.append("i.locale IN :locales")
    params["locales"] = tuple(sel_locales)
if sel_categories:
    where.append("i.category IN :categories")
    params["categories"] = tuple(sel_categories)
if keyword.strip():
    where.append("(i.description LIKE :kw OR i.cause LIKE :kw OR i.response LIKE :kw OR i.note LIKE :kw)")
    params["kw"] = f"%{keyword.strip()}%"
where_sql = " AND ".join(where)

# ---------------------------
# 데이터 쿼리 함수
# ---------------------------
@st.cache_data(ttl=60, show_spinner=False)
def fetch_kpis(where_sql: str, params: dict):
    # 전체 건수, 오늘 건수, 카테고리 상위 1, 플랫폼별 건수
    with engine.connect() as conn:
        total = pd.read_sql(text(f"SELECT COUNT(*) AS cnt FROM incidents i WHERE {where_sql}"), conn, params=params)["cnt"].iloc[0]
        today_params = dict(params)
        today_params.update({
            "date_from": datetime.combine(datetime.now().date(), datetime.min.time()),
            "date_to": datetime.combine(datetime.now().date(), datetime.max.time())
        })
        today_cnt = pd.read_sql(text(f"SELECT COUNT(*) AS cnt FROM incidents i WHERE {where_sql}"), conn, params=today_params)["cnt"].iloc[0]
        top_cat_df = pd.read_sql(text(f"SELECT i.category, COUNT(*) AS cnt FROM incidents i WHERE {where_sql} GROUP BY i.category ORDER BY cnt DESC LIMIT 1"), conn, params=params)
        top_cat = (top_cat_df["category"].iloc[0], int(top_cat_df["cnt"].iloc[0])) if not top_cat_df.empty else ("-", 0)
        plat_df = pd.read_sql(text(f"SELECT i.platform, COUNT(*) AS cnt FROM incidents i WHERE {where_sql} GROUP BY i.platform ORDER BY cnt DESC"), conn, params=params)
    return total, today_cnt, top_cat, plat_df

@st.cache_data(ttl=60, show_spinner=False)
def fetch_timeseries(where_sql: str, params: dict) -> pd.DataFrame:
    sql = text(f"""
        SELECT DATE(i.started_at) AS d, COUNT(*) AS cnt
        FROM incidents i
        WHERE {where_sql}
        GROUP BY DATE(i.started_at)
        ORDER BY d
    """)
    with engine.connect() as conn:
        return pd.read_sql(sql, conn, params=params)

@st.cache_data(ttl=60, show_spinner=False)
def fetch_list(where_sql: str, params: dict, limit: int) -> pd.DataFrame:
    sql = text(f"""
        SELECT i.id, i.started_at, i.ended_at, i.duration, i.platform, i.locale, i.inquiry_count,
               i.category, i.description, i.cause, i.response, i.note, i.created_at, i.updated_at
        FROM incidents i
        WHERE {where_sql}
        ORDER BY i.started_at DESC
        LIMIT :limit
    """)
    p = dict(params)
    p["limit"] = int(limit)
    with engine.connect() as conn:
        df = pd.read_sql(sql, conn, params=p)
    if not df.empty:
        df["started_at"] = pd.to_datetime(df["started_at"]).dt.strftime("%Y-%m-%d %H:%M")
        df["ended_at"]   = df["ended_at"].apply(lambda x: "" if pd.isna(x) else pd.to_datetime(x).strftime("%Y-%m-%d %H:%M"))
    return df

# ---------------------------
# KPI 카드 렌더링
# ---------------------------
col1, col2, col3, col4 = st.columns(4)
try:
    total, today_cnt, (top_cat_name, top_cat_cnt), plat_df = fetch_kpis(where_sql, params)
    col1.metric("총 건수", f"{total:,}")
    col2.metric("오늘 건수", f"{today_cnt:,}")
    col3.metric("최다 카테고리", f"{top_cat_name}", delta=f"{top_cat_cnt:,}건")
    # 플랫폼별 bar (간단 KPI)
    if not plat_df.empty:
        with col4:
            st.write("플랫폼별")
            chart = alt.Chart(plat_df).mark_bar().encode(x=alt.X('platform:N', sort='-y'), y='cnt:Q', tooltip=['platform','cnt'])
            st.altair_chart(chart, use_container_width=True)
except Exception as e:
    st.warning(f"KPI 로딩 오류: {e}")

# ---------------------------
# 일별 추이 차트
# ---------------------------
st.subheader("📈 일별 발생 추이")
ts_df = fetch_timeseries(where_sql, params)
if ts_df.empty:
    st.info("데이터가 없습니다.")
else:
    chart = alt.Chart(ts_df).mark_line(point=True).encode(x='d:T', y='cnt:Q', tooltip=['d:T','cnt:Q'])
    st.altair_chart(chart, use_container_width=True)

# ---------------------------
# 목록 & 선택 삭제
# ---------------------------
st.subheader("📄 사건 목록")
list_df = fetch_list(where_sql, params, int(limit))
if list_df.empty:
    st.info("조건에 맞는 데이터가 없습니다.")
else:
    st.dataframe(list_df, use_container_width=True, height=420)
    # 삭제 기능
    with st.expander("관리: 선택 삭제"):
        ids = st.text_input("삭제할 ID 목록(쉼표로 구분)", placeholder="예: 101,102,120")
        if st.button("삭제 실행", type="primary"):
            try:
                id_list = [int(x.strip()) for x in ids.split(',') if x.strip()]
                if not id_list:
                    st.warning("ID를 입력하세요.")
                else:
                    q = text("DELETE FROM incidents WHERE id IN :ids")
                    with engine.begin() as conn:
                        conn.execute(q, {"ids": tuple(id_list)})
                    st.success(f"삭제 완료: {len(id_list)}건")
                    st.cache_data.clear()
            except Exception as e:
                st.error(f"삭제 중 오류: {e}")

# ---------------------------
# 관리: 업로드(엑셀/CSV)
# ---------------------------
st.subheader("🛠 관리: 업로드")
with st.expander("엑셀/CSV 업로드"):
    file = st.file_uploader("파일 선택", type=["csv", "xlsx", "xls"])
    hint = st.caption("열 이름은 가능하면 다음에 맞춰주세요: started_at, ended_at, duration, platform, locale, inquiry_count, category, description, cause, response, note")

    def normalize_df(df: pd.DataFrame) -> pd.DataFrame:
        # 컬럼 소문자화 & 양끝 공백 제거
        df.columns = [str(c).strip().lower() for c in df.columns]
        # 날짜 컬럼 파싱
        for col in ["started_at", "ended_at"]:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')
        # 타입 캐스팅
        if "inquiry_count" in df.columns:
            df["inquiry_count"] = pd.to_numeric(df["inquiry_count"], errors='coerce')
        # 누락 컬럼 채우기
        for col in ["duration","platform","locale","category","description","cause","response","note"]:
            if col not in df.columns:
                df[col] = None
        # 필수 컬럼 체크
        required = ["started_at", "category", "description"]
        missing = [c for c in required if c not in df.columns]
        if missing:
            raise ValueError(f"필수 컬럼 누락: {missing}")
        return df[["started_at","ended_at","duration","platform","locale","inquiry_count","category","description","cause","response","note"]]

    if file is not None:
        try:
            if file.name.lower().endswith('.csv'):
                df_up = pd.read_csv(file)
            else:
                df_up = pd.read_excel(file)
            df_up = normalize_df(df_up)
            # 업로드
            with engine.begin() as conn:
                df_up.to_sql('incidents', conn, if_exists='append', index=False)
            st.success(f"업로드 완료: {len(df_up)}건")
            st.cache_data.clear()
        except Exception as e:
            st.error(f"업로드 실패: {e}")

st.markdown("\n—\n💡 KPI 카드는 총 건수/오늘 건수/최다 카테고리/플랫폼별 분포를 보여줍니다. 필요 시 Severity, 상태, 서비스 영역 차원을 추가해 확장 가능합니다.")
