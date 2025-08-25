# app_topcombo_master_detail.py
# 좌: 카테고리 차트 / 우: KPI(가로 3개)
# 목록: 마스터/디테일(행 클릭으로 상세 펼침)
# 날짜: 시작/종료일 개별 입력
# created_at / updated_at 숨김
# 차트: 색상+라벨, 총건수 강조

import os
import pandas as pd
import altair as alt
import streamlit as st
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode, JsCode

# ---------------------------
# 페이지 설정 + 상단 타이틀(작게)
# ---------------------------
st.set_page_config(page_title="장애현황", page_icon="📊", layout="wide")
st.markdown("<h1 style='font-size:2rem;margin:0 0 0.5rem 0;'>📊 장애현황</h1>", unsafe_allow_html=True)
# 설명 캡션은 삭제

# --- 표/에디터 셀 줄바꿈 + AgGrid 폰트 축소 ---
st.markdown(
    """
<style>
/* DataFrame / DataEditor 줄바꿈 유지 */
[data-testid="stDataFrame"] div[role="gridcell"],
[data-testid="stDataEditor"] div[role="gridcell"]{
  white-space: pre-wrap !important;
  line-height: 1.3;
}
/* AgGrid 전체 폰트 축소 */
.ag-theme-streamlit { --ag-font-size: 12px; }
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
        pw   = cfg.get("DB_PASSWORD") or cfg.get("DB_PASSWORD")
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
    return total, today_cnt, top_cat

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
        df["desc_one"] = df["description"].astype(str).str.split("\n").str[0]
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

# ===========================
# 요약: (좌) 카테고리 그래프 / (우) KPI(가로 3개, 가운데 정렬)
# ===========================
st.subheader("요약")

# KPI 먼저 계산
total, today_cnt, top_cat_name, top_cat_cnt = 0, 0, "-", 0
try:
    _t, _td, (cat_name, cat_cnt) = fetch_kpis(where_sql, params)
    total, today_cnt, top_cat_name, top_cat_cnt = int(_t), int(_td), str(cat_name), int(cat_cnt)
except Exception as e:
    st.warning(f"KPI 로딩 오류: {e}")

# 좌/우 50% 배치
col_chart, col_kpi = st.columns([1, 1])

with col_chart:
    cat_df = fetch_category_counts(where_sql, params)
    if cat_df.empty:
        st.info("카테고리 데이터가 없습니다.")
    else:
        # 총건수 한 줄 추가
        total_df   = pd.DataFrame([{"category": "총건수", "cnt": int(cat_df["cnt"].sum())}])
        cat_sorted = cat_df.sort_values("cnt", ascending=False).reset_index(drop=True)

        order   = ["총건수"] + cat_sorted["category"].tolist()
        plot_df = pd.concat([total_df, cat_sorted], ignore_index=True)

        base = alt.Chart(plot_df).encode(
            y=alt.Y("category:N", sort=order, title=""),
            x=alt.X("cnt:Q", title="건수",
                    axis=alt.Axis(format="d", tickMinStep=1))  # 🔹 정수 표시
        )

        bars = base.mark_bar().encode(
            color=alt.condition(
                alt.datum.category == "총건수",
                alt.value("#1d4ed8"),   # 총건수: 진한 블루
                alt.value("#3b82f6")    # 나머지: 기본 블루
            ),
            tooltip=[
                alt.Tooltip("category:N", title="구분"),
                alt.Tooltip("cnt:Q", title="건수")
            ]
        )

        labels = base.mark_text(
            align="left", baseline="middle", dx=4,
            fontSize=12, color="white", fontWeight="bold"
        ).encode(text=alt.Text("cnt:Q", format=",.0f"))

        # ▶ 막대 개수가 적어도 너무 낮아지지 않도록 최소 높이 보장
        min_h   = 180
        auto_h  = 28 * len(plot_df)
        height  = max(min_h, auto_h)

        st.altair_chart((bars + labels).properties(height=height, width="container"),
                        use_container_width=True)

with col_kpi:
    st.markdown(
        """
        <style>
          .kpi-grid{display:flex; gap:8px;}
          .kpi-card{
              flex:1; text-align:center;
              border:1px solid rgba(255,255,255,0.15);
              border-radius:10px; padding:8px 6px;
              background:rgba(255,255,255,0.03);
          }
          .kpi-title{ font-size:14px; margin:0; opacity:0.85; }
          .kpi-value{ font-size:22px; margin:4px 0; font-weight:700; }
          .kpi-sub{ font-size:12px; opacity:0.7; margin-top:-2px; }
          /* 총건수 강조(볼드 + 진한 블루) */
          .kpi-card:first-child .kpi-title,
          .kpi-card:first-child .kpi-value { font-weight:900; color:#1d4ed8; }
        </style>
        """,
        unsafe_allow_html=True
    )
    st.markdown(
        f"""
        <div class="kpi-grid">
          <div class="kpi-card">
            <div class="kpi-title">총 건수</div>
            <div class="kpi-value">{total:,}</div>
          </div>
          <div class="kpi-card">
            <div class="kpi-title">오늘 건수</div>
            <div class="kpi-value">{today_cnt:,}</div>
          </div>
          <div class="kpi-card">
            <div class="kpi-title">최다 카테고리</div>
            <div class="kpi-value">{top_cat_name}</div>
            <div class="kpi-sub">{top_cat_cnt:,}건</div>
          </div>
        </div>
        """,
        unsafe_allow_html=True
    )

# ---------------------------
# (요청: 일별 발생 추이 숨김) — 해당 섹션 제거
# ---------------------------

# -----------------------------------------------------------------------------
# 장애 리스트 (마스터/디테일: 행 클릭으로 상세 펼치기) + 폰트 축소
# -----------------------------------------------------------------------------
st.subheader("📄 장애 리스트")

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
        rowHeight=34,  # 조금 더 컴팩트
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
        enable_enterprise_modules=True,
    )

# ---------------------------
# 선택삭제
# ---------------------------
with st.expander("🗑 선택삭제"):
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
# 오류추가 (웹에서 직접 입력)
# ---------------------------
with st.expander("➕ 오류추가"):
    st.caption("아래 항목을 입력 후 [저장]을 누르면 DB에 바로 추가됩니다.")
    
    with st.form("add_incident_form", clear_on_submit=False):
        # ── 기본 시간 입력
        now = datetime.now().replace(second=0, microsecond=0)
        c1, c2, c3, c4 = st.columns(4)
        with c1:
            s_date = st.date_input("시작일", value=now.date(), key="s_date_in_form")
        with c2:
            s_time = st.time_input("시작시간", value=now.time(), key="s_time_in_form")
        with c3:
            use_end = st.checkbox("종료일시 입력", value=False, key="use_end_endtime")
        with c4:
            duration = st.text_input("장애시간(예: 2h 30m / 45m)", placeholder="선택")

        if use_end:
            c5, c6 = st.columns(2)
            with c5:
                e_date = st.date_input("종료일", value=now.date(), key="e_date_in_form")
            with c6:
                e_time = st.time_input("종료시간", value=now.time(), key="e_time_in_form")
        else:
            e_date, e_time = None, None

    # ── 분류/메타 (단일 선택: 현재 스키마 호환)
        c7, c8, c9, c10 = st.columns(4)
        with c7:
            platform = st.selectbox("플랫폼", options=(["ALL"] + [x for x in PLATFORMS if x]))
        with c8:
            locale = st.selectbox("로케일", options=(["KR","JP","US","ALL"] + [x for x in LOCALES if x not in ["KR","JP","US","ALL"]]))
        with c9:
            inquiry_count = st.number_input("문의량", min_value=0, step=1, value=0)
        with c10:
            category = st.selectbox("카테고리", options=CATEGORIES)  # ← 저장된 목록에서만 선택

    # ── 본문
        description = st.text_area("장애 내용 (필수)", height=120, placeholder="무슨 현상이 언제/어디서 발생했는지")
        cause       = st.text_area("원인", height=100, placeholder="원인 분석/추정")
        response    = st.text_area("대응", height=100, placeholder="조치 내역/연표")
        note        = st.text_area("비고", height=80, placeholder="관련 링크 등")

        saved = st.form_submit_button("저장", type="primary")

        if saved:
        # ── 유효성 체크
            errors = []
            if not description.strip():
                errors.append("장애 내용은 필수입니다.")
            if not (category and str(category).strip()):
                errors.append("카테고리는 필수입니다.")

            try:
                started_at = datetime.combine(s_date, s_time)
            except Exception:
                errors.append("시작일시가 올바르지 않습니다.")

            ended_at = None
            if use_end:
                try:
                    ended_at = datetime.combine(e_date, e_time)
                except Exception:
                    errors.append("종료일시가 올바르지 않습니다.")

            if errors:
                for msg in errors:
                    st.error(msg)
            else:
                try:
                    payload = {
                        "started_at": started_at,
                        "ended_at": ended_at,
                        "duration": duration.strip() or None,
                        "platform": platform,
                        "locale": locale,
                        "inquiry_count": int(inquiry_count) if inquiry_count is not None else 0,
                        "category": category,
                        "description": description.strip(),
                        "cause": cause.strip() or None,
                        "response": response.strip() or None,
                        "note": note.strip() or None,
                    }
                    with engine.begin() as conn:
                        conn.execute(
                            text("""
                                INSERT INTO incidents
                                (started_at, ended_at, duration, platform, locale, inquiry_count,
                                 category, description, cause, response, note)
                                VALUES
                                (:started_at, :ended_at, :duration, :platform, :locale, :inquiry_count,
                                 :category, :description, :cause, :response, :note)
                            """),
                            payload
                        )
                    st.success("오류 현황이 저장되었습니다 ✅")
                    st.cache_data.clear()
                except Exception as e:
                    st.error(f"저장 중 오류가 발생했습니다: {e}")
                    
# ---------------------------
# 파일업로드 (CSV/엑셀)
# ---------------------------
with st.expander("⬆️ 파일업로드"):
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
