# app_kpi_master_detail_with_category_chart.py
# KPI 카드 + 필터 + 업로드/삭제 + 마스터/디테일(행 클릭) + 카테고리 막대(색상+라벨)

import os
import pandas as pd
import altair as alt
import streamlit as st
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode, JsCode

st.set_page_config(page_title="장애 현황 대시보드", page_icon="📊", layout="wide")
st.title("📊 장애 현황 대시보드")
st.caption("KPI 카드 · 필터 · 업로드/삭제 관리 · 마스터/디테일(행 클릭) · 카테고리 막대(색상+라벨)")

# --- 표/에디터 셀 줄바꿈 보존 ---
st.markdown(
    """
<style>
[data-testid="stDataFrame"] div[role="gridcell"],
[data-testid="stDataEditor"] div[role="gridcell"]{
  white-space: pre-wrap !important;
  line-height: 1.3;
}
</style>
""",
    unsafe_allow_html=True,
)

# ---------------------------
# DB 연결
# ---------------------------
@st.cache_resource(show_spinner=False)
def get_engine():
    s = st.secrets
    if "db" in s:
        cfg = s["db"];  host = cfg.get("HOST") or cfg.get("host")
        port = int(cfg.get("PORT") or cfg.get("port") or 3306)
        user = cfg.get("USER") or cfg.get("user")
        pw   = cfg.get("PASSWORD") or cfg.get("password")
        name = cfg.get("NAME") or cfg.get("name")
    elif "DB" in s:
        cfg = s["DB"];  host = cfg.get("DB_HOST") or cfg.get("HOST")
        port = int(cfg.get("DB_PORT") or cfg.get("PORT") or 3306)
        user = cfg.get("DB_USER") or cfg.get("USER")
        pw   = cfg.get("DB_PASSWORD") or cfg.get("PASSWORD")
        name = cfg.get("DB_NAME") or cfg.get("NAME")
    else:
        host = os.getenv("DB_HOST"); port = int(os.getenv("DB_PORT") or 3306)
        user = os.getenv("DB_USER"); pw = os.getenv("DB_PASSWORD"); name = os.getenv("DB_NAME")

    if not all([host, user, pw, name]):
        st.error("DB secrets가 없습니다. [db] 또는 [DB] 섹션으로 HOST/PORT/USER/PASSWORD/NAME을 등록하세요.")
        st.stop()

    url = f"mysql+pymysql://{user}:{pw}@{host}:{port}/{name}?charset=utf8mb4"
    connect_args = {"ssl": {"ssl": True}}
    eng = create_engine(url, pool_pre_ping=True, connect_args=connect_args)
    with eng.connect() as conn:
        conn.execute(text("SELECT 1"))
    return eng

engine = get_engine()

# ---------------------------
# 쿼리 유틸
# ---------------------------
@st.cache_data(ttl=180, show_spinner=False)
def get_distinct_values():
    with engine.connect() as conn:
        platforms = pd.read_sql(text(
            "SELECT DISTINCT platform FROM incidents WHERE platform<>'' AND platform IS NOT NULL ORDER BY platform"
        ), conn)["platform"].tolist()
        locales   = pd.read_sql(text(
            "SELECT DISTINCT locale FROM incidents WHERE locale<>''   AND locale   IS NOT NULL ORDER BY locale"
        ), conn)["locale"].tolist()
        cats      = pd.read_sql(text(
            "SELECT DISTINCT category FROM incidents WHERE category<>'' AND category IS NOT NULL ORDER BY category"
        ), conn)["category"].tolist()
    return platforms, locales, cats

@st.cache_data(ttl=90, show_spinner=False)
def fetch_kpis(where_sql: str, params: dict):
    with engine.connect() as conn:
        total = pd.read_sql(text(f"SELECT COUNT(*) cnt FROM incidents i WHERE {where_sql}"),
                            conn, params=params)["cnt"].iloc[0]
        tparams = dict(params)
        tparams["date_from"] = datetime.combine(datetime.now().date(), datetime.min.time())
        tparams["date_to"]   = datetime.combine(datetime.now().date(), datetime.max.time())
        today_cnt = pd.read_sql(text(f"SELECT COUNT(*) cnt FROM incidents i WHERE {where_sql}"),
                                conn, params=tparams)["cnt"].iloc[0]
        top_cat_df = pd.read_sql(text(
            f"SELECT i.category, COUNT(*) cnt FROM incidents i WHERE {where_sql} "
            "GROUP BY i.category ORDER BY cnt DESC LIMIT 1"
        ), conn, params=params)
        top_cat = (top_cat_df["category"].iloc[0], int(top_cat_df["cnt"].iloc[0])) if not top_cat_df.empty else ("-", 0)
        plat_df = pd.read_sql(text(
            f"SELECT i.platform, COUNT(*) cnt FROM incidents i WHERE {where_sql} "
            "GROUP BY i.platform ORDER BY cnt DESC"
        ), conn, params=params)
    return total, today_cnt, top_cat, plat_df

@st.cache_data(ttl=90, show_spinner=False)
def fetch_timeseries(where_sql: str, params: dict) -> pd.DataFrame:
    with engine.connect() as conn:
        return pd.read_sql(text(
            f"SELECT DATE(i.started_at) d, COUNT(*) cnt FROM incidents i WHERE {where_sql} "
            "GROUP BY DATE(i.started_at) ORDER BY d"
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
        for col in ["description", "cause", "response", "note"]:
            if col in df.columns:
                df[col] = df[col].astype(str).str.replace("\r\n", "\n").str.replace("\r", "\n")
        df["desc_one"] = df["description"].astype(str).str.split("\n").str[0]  # 요약 1줄
    return df

@st.cache_data(ttl=90, show_spinner=False)
def fetch_category_counts(where_sql: str, params: dict) -> pd.DataFrame:
    with engine.connect() as conn:
        df = pd.read_sql(
            text(f"SELECT i.category, COUNT(*) cnt FROM incidents i WHERE {where_sql} GROUP BY i.category"),
            conn, params=params
        )
    return df

PLATFORMS, LOCALES, CATEGORIES = get_distinct_values()

# ---------------------------
# 사이드바 필터 (시작/종료일 개별 입력)
# ---------------------------
with st.sidebar:
    st.header("필터")
    today = datetime.now().date()
    start_default = st.session_state.get("start_date", today - timedelta(days=30))
    end_default   = st.session_state.get("end_date", today)

    start_date = st.date_input("시작일 (started_at)", value=start_default, key="start_date")
    end_date   = st.date_input("종료일 (started_at)", value=end_default, min_value=start_date, key="end_date")
    if end_date < start_date:
        st.warning("종료일이 시작일보다 빠릅니다. 시작일로 보정합니다.")
        end_date = start_date
        st.session_state["end_date"] = end_date

    sel_platforms  = st.multiselect("플랫폼", options=PLATFORMS)
    sel_locales    = st.multiselect("로케일", options=LOCALES)
    sel_categories = st.multiselect("카테고리", options=CATEGORIES)
    keyword        = st.text_input("키워드(내용/원인/대응/비고)")
    limit          = st.number_input("목록 행수", min_value=50, max_value=5000, value=500, step=50)

params = {
    "date_from": datetime.combine(start_date, datetime.min.time()),
    "date_to":   datetime.combine(end_date,   datetime.max.time()),
}
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

# ---------------------------
# KPI 카드
# ---------------------------
try:
    total, today_cnt, (top_cat_name, top_cat_cnt), plat_df = fetch_kpis(where_sql, params)
    c1, c2, c3 = st.columns([1,1,1])
    c1.metric("총 건수", f"{total:,}")
    c2.metric("오늘 건수", f"{today_cnt:,}")
    c3.metric("최다 카테고리", top_cat_name, delta=f"{top_cat_cnt:,}건")
except Exception as e:
    st.warning(f"KPI 로딩 오류: {e}")

# ---------------------------
# 카테고리별 통계 (총건수 + 가로바, 위: 총건수 / 아래: 최소 건수)
# ---------------------------
st.subheader("📊 카테고리별 통계 (총건수 상단, 최소 건수 하단)")

cat_df = fetch_category_counts(where_sql, params)
if cat_df.empty:
    st.info("카테고리 데이터가 없습니다.")
else:
    total_df = pd.DataFrame([{"category": "총건수", "cnt": int(cat_df["cnt"].sum())}])
    cat_sorted = cat_df.sort_values("cnt", ascending=False).reset_index(drop=True)
    order = ["총건수"] + cat_sorted["category"].tolist()
    plot_df = pd.concat([total_df, cat_sorted], ignore_index=True)

    # 가로 막대 + 라벨(막대 끝에 건수 표시)
    base = alt.Chart(plot_df).encode(
        y=alt.Y("category:N", sort=order, title=""),
        x=alt.X("cnt:Q", title="건수")
    )

    bars = base.mark_bar().encode(
        color=alt.Color(
            "category:N",
            legend=None,
            scale=alt.Scale(range=['#3b82f6'] + ['#60a5fa'] * (len(plot_df)-1))  # 총건수 진한색, 나머지 연한색
        ),
        tooltip=[alt.Tooltip("category:N", title="구분"), alt.Tooltip("cnt:Q", title="건수")]
    )

    labels = base.mark_text(
        align='left',
        baseline='middle',
        dx=4,
        fontSize=12,
        color='white'
    ).encode(
        text=alt.Text("cnt:Q", format=",.0f")
    )

    st.altair_chart((bars + labels).properties(height=28 * len(plot_df), width="container"), use_container_width=True)

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

# -----------------------------------------------------------------------------
# 목록 (마스터/디테일: 행 클릭 → 그 행 아래로 상세 펼치기)
# -----------------------------------------------------------------------------
st.subheader("📄 사건 목록 (행 클릭으로 상세 펼치기)")

list_df = fetch_list(where_sql, params, int(limit))
if list_df.empty:
    st.info("조건에 맞는 데이터가 없습니다.")
else:
    md_df = list_df.copy()
    md_df["desc_one"] = md_df["description"].astype(str).str.split("\n").str[0]

    gb = GridOptionsBuilder.from_dataframe(md_df)
    gb.configure_grid_options(
        masterDetail=True,
        detailRowAutoHeight=True,
        detailRowHeight=220,
        rowHeight=36,
        onRowClicked=JsCode("function(e){ e.node.setExpanded(!e.node.expanded); }"),
        suppressRowClickSelection=False,
        suppressCellSelection=True,
    )

    # 마스터(요약) 보이는 컬럼
    gb.configure_column("desc_one", header_name="description",
                        cellStyle={"whiteSpace": "nowrap", "textOverflow": "ellipsis", "overflow": "hidden"},
                        width=420)
    gb.configure_column("started_at", width=140)
    gb.configure_column("ended_at",   width=140)
    gb.configure_column("category",   width=120)
    gb.configure_column("platform",   width=90)
    gb.configure_column("locale",     width=70)
    gb.configure_column("inquiry_count", header_name="문의량", width=80)

    # 디테일로 넘길 원문·관리 컬럼은 마스터에서 숨김
    for col in ["description", "cause", "response", "note", "created_at", "updated_at"]:
        if col in md_df.columns:
            gb.configure_column(col, hide=True)

    # 디테일 그리드
    detail_col_defs = [
        {"field": "description", "headerName": "장애내용",
         "wrapText": True, "autoHeight": True,
         "cellStyle": {"white-space": "pre-wrap", "line-height": "1.3"}},
        {"field": "cause", "headerName": "원인",
         "wrapText": True, "autoHeight": True,
         "cellStyle": {"white-space": "pre-wrap", "line-height": "1.3"}},
        {"field": "response", "headerName": "대응",
         "wrapText": True, "autoHeight": True,
         "cellStyle": {"white-space": "pre-wrap", "line-height": "1.3"}},
        {"field": "note", "headerName": "비고",
         "wrapText": True, "autoHeight": True,
         "cellStyle": {"white-space": "pre-wrap", "line-height": "1.3"}},
    ]
    detail_grid_options = {
        "defaultColDef": {"flex": 1, "sortable": False, "filter": False, "resizable": True},
        "columnDefs": detail_col_defs,
        "suppressCellSelection": True,
        "rowHeight": 24,
    }

    gb.configure_grid_options(
        detailCellRendererParams={
            "detailGridOptions": detail_grid_options,
            "getDetailRowData": JsCode("function(params){ params.successCallback([params.data]); }"),
        }
    )

    grid = AgGrid(
        md_df[[
            "id", "started_at", "ended_at", "duration",
            "platform", "locale", "inquiry_count", "category", "desc_one",
            # detail 전달용 숨김 컬럼
            "description", "cause", "response", "note", "created_at", "updated_at"
        ]],
        gridOptions=gb.build(),
        theme="streamlit",
        height=560,
        allow_unsafe_jscode=True,
        update_mode=GridUpdateMode.NO_UPDATE,
        enable_enterprise_modules=True,  # masterDetail 활성화
    )

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
        df.columns = [str(c).strip().lower() for c in df.columns]
        for dt in ["started_at", "ended_at"]:
            if dt in df.columns:
                df[dt] = pd.to_datetime(df[dt], errors="coerce")
        if "inquiry_count" in df.columns:
            df["inquiry_count"] = pd.to_numeric(df["inquiry_count"], errors="coerce")
        for col in ["ended_at","duration","platform","locale","inquiry_count","cause","response","note"]:
            if col not in df.columns:
                df[col] = None
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
