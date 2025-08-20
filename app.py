import pandas as pd
import altair as alt
import streamlit as st
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text

st.set_page_config(page_title="Incidents Dashboard", page_icon="🚨", layout="wide")
st.title("🚨 Incidents Dashboard")

# ---------------------------
# DB 연결 (secrets 사용)
# ---------------------------
@st.cache_resource(show_spinner=False)
def get_engine():
    cfg = st.secrets["db"]
    url = (
        f"mysql+pymysql://{cfg['USER']}:{cfg['PASSWORD']}"
        f"@{cfg['HOST']}:{int(cfg['PORT'])}/{cfg['NAME']}?charset=utf8mb4"
    )
    return create_engine(url, pool_pre_ping=True)

engine = get_engine()

# ---------------------------
# 사이드바 필터
# ---------------------------
with st.sidebar:
    st.header("필터")
    today = datetime.now().date()
    default_from = today - timedelta(days=30)

    # started_at 기준 기간 필터
    date_from, date_to = st.date_input(
        "기간(started_at 기준)",
        value=(default_from, today)
    )
    if isinstance(date_from, tuple):
        date_from, date_to = date_from

    keyword = st.text_input("키워드(장애내용/원인/대응)", placeholder="예: 로그인 불가")
    sel_category = st.text_input("카테고리 정확히 일치(선택)", placeholder="예: 가입/로그인")
    limit = st.number_input("목록 행수(limit)", min_value=50, max_value=5000, value=500, step=50)

params = {
    "date_from": datetime.combine(date_from, datetime.min.time()),
    "date_to":   datetime.combine(date_to,   datetime.max.time()),
    "limit": int(limit)
}

# ---------------------------
# 동적 WHERE 구성
# ---------------------------
where = ["started_at BETWEEN :date_from AND :date_to"]
if sel_category.strip():
    where.append("category = :category")
    params["category"] = sel_category.strip()
if keyword.strip():
    # 간단 like 검색(필요시 FULLTEXT 컬럼 추가해 개선)
    where.append("(description LIKE :kw OR cause LIKE :kw OR response LIKE :kw OR note LIKE :kw)")
    params["kw"] = f"%{keyword.strip()}%"

where_sql = " AND ".join(where)

# ---------------------------
# 쿼리 함수
# ---------------------------
@st.cache_data(ttl=60, show_spinner=False)
def fetch_counts_by_category(where_sql: str, params: dict) -> pd.DataFrame:
    sql = text(f"""
        SELECT category, COUNT(*) AS cnt
        FROM incidents
        WHERE {where_sql}
        GROUP BY category
        ORDER BY cnt DESC
    """)
    with engine.connect() as conn:
        return pd.read_sql(sql, conn, params=params)

@st.cache_data(ttl=60, show_spinner=False)
def fetch_timeseries(where_sql: str, params: dict) -> pd.DataFrame:
    sql = text(f"""
        SELECT DATE(started_at) AS d, COUNT(*) AS cnt
        FROM incidents
        WHERE {where_sql}
        GROUP BY DATE(started_at)
        ORDER BY d
    """)
    with engine.connect() as conn:
        return pd.read_sql(sql, conn, params=params)

@st.cache_data(ttl=60, show_spinner=False)
def fetch_list(where_sql: str, params: dict) -> pd.DataFrame:
    sql = text(f"""
        SELECT id, started_at, ended_at, duration, platform, locale, inquiry_count,
               category, description, cause, response, note, created_at, updated_at
        FROM incidents
        WHERE {where_sql}
        ORDER BY started_at DESC
        LIMIT :limit
    """)
    with engine.connect() as conn:
        df = pd.read_sql(sql, conn, params=params)
    if not df.empty:
        df["started_at"] = pd.to_datetime(df["started_at"]).dt.strftime("%Y-%m-%d %H:%M")
        df["ended_at"]   = df["ended_at"].apply(lambda x: "" if pd.isna(x) else pd.to_datetime(x).strftime("%Y-%m-%d %H:%M"))
    return df

# ---------------------------
# 상단 KPI & 차트
# ---------------------------
c1, c2 = st.columns([1, 2])

with c1:
    st.subheader("카테고리별 건수")
    cat = fetch_counts_by_category(where_sql, params)
    if cat.empty:
        st.info("데이터가 없습니다.")
    else:
        st.dataframe(cat, hide_index=True, use_container_width=True)

with c2:
    st.subheader("일별 추이")
    ts = fetch_timeseries(where_sql, params)
    if ts.empty:
        st.info("데이터가 없습니다.")
    else:
        chart = alt.Chart(ts).mark_line(point=True).encode(
            x="d:T", y="cnt:Q", tooltip=["d:T","cnt:Q"]
        )
        st.altair_chart(chart, use_container_width=True)

# ---------------------------
# 목록 & 상세
# ---------------------------
st.subheader("사건 목록")
df = fetch_list(where_sql, params)
if df.empty:
    st.info("조건에 맞는 데이터가 없습니다.")
else:
    st.dataframe(df, use_container_width=True, height=480)

    st.markdown("---")
    st.subheader("상세 보기")
    sel_id = st.selectbox("Incident 선택", options=df["id"].tolist())
    if sel_id:
        @st.cache_data(ttl=60, show_spinner=False)
        def fetch_detail(iid: int) -> pd.Series:
            sql = text("""
                SELECT *
                FROM incidents
                WHERE id = :id
            """)
            with engine.connect() as conn:
                detail = pd.read_sql(sql, conn, params={"id": int(iid)})
            return detail.iloc[0] if not detail.empty else None

        row = fetch_detail(sel_id)
        if row is not None:
            st.write(f"**ID**: {int(row['id'])}")
            st.write(f"**시작**: {row['started_at']}")
            st.write(f"**종료**: {row['ended_at']}")
            st.write(f"**장애시간**: {row['duration']}")
            st.write(f"**플랫폼/로케일**: {row['platform']} / {row['locale']}")
            st.write(f"**문의량**: {row['inquiry_count']}")
            st.write(f"**카테고리**: {row['category']}")
            st.write("**장애내용**")
            st.write(row['description'])
            st.write("**원인**")
            st.write(row['cause'])
            st.write("**대응**")
            st.write(row['response'])
            st.write("**비고**")
            st.write(row['note'])
