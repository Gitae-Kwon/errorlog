import os
from datetime import datetime, timedelta
from typing import Optional, List

import streamlit as st
from sqlalchemy import (
    create_engine, Column, Integer, String, Text, DateTime, func, select, and_, or_, text, UniqueConstraint
)
from sqlalchemy.orm import sessionmaker, declarative_base

# -----------------------------
# DB 연결 설정
# -----------------------------
# ▶ .streamlit/secrets.toml 예시
# [db]
# url = "postgresql+psycopg://USER:PASSWORD@HOST:PORT/DBNAME"  # PostgreSQL
# # 또는
# # url = "mysql+pymysql://USER:PASSWORD@HOST:PORT/DBNAME?charset=utf8mb4"  # MySQL

DB_URL = st.secrets.get("db", {}).get("url", os.getenv("DB_URL", ""))
if not DB_URL:
    st.stop()

@st.cache_resource(show_spinner=False)
def get_engine():
    return create_engine(DB_URL, pool_pre_ping=True)

engine = get_engine()
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)
Base = declarative_base()

# -----------------------------
# ORM 모델 (한국어 컬럼명을 DB에 유지)
# -----------------------------
class Incident(Base):
    __tablename__ = "incidents"

    id = Column(Integer, primary_key=True)
    started_at = Column("발생일시_start", DateTime, nullable=False)
    ended_at = Column("발생일시_end", DateTime)
    inquiries = Column("문의량", Integer)
    locale = Column("로케일", String(100))
    platform = Column("플랫폼", String(100))
    category = Column("유형", String(100))
    incident_type = Column("장애구분", String(255))
    cause = Column("원인", Text)
    impact = Column("영향", Text)
    follow_up = Column("다음이력", Text)
    ref_link = Column("참고링크", String(500))
    memo = Column("비고", Text)
    created_at = Column(DateTime, server_default=func.current_timestamp())
    updated_at = Column(DateTime, server_default=func.current_timestamp(), onupdate=func.current_timestamp())

    __table_args__ = (
        UniqueConstraint("발생일시_start", "플랫폼", "유형", name="uq_incident"),
    )

# -----------------------------
# 유틸: 선택지 로딩
# -----------------------------
@st.cache_data(ttl=600)
def distinct_values(column_sql_name: str) -> List[str]:
    with SessionLocal() as s:
        q = s.execute(text(f"SELECT DISTINCT `{column_sql_name}` AS val FROM incidents WHERE `{column_sql_name}` IS NOT NULL AND `{column_sql_name}` <> '' ORDER BY `{column_sql_name}`"))
        # 위 쿼리는 MySQL 백틱 기준. PostgreSQL에서도 동작하도록 식별자 따옴표 처리
        # 간단 호환: 백틱을 쌍따옴표로 교체 (psycopg/pg에서는 "컬럼")
        if engine.url.get_backend_name().startswith("postgresql"):
            q = s.execute(text(f'SELECT DISTINCT "{column_sql_name}" AS val FROM incidents WHERE "{column_sql_name}" IS NOT NULL AND "{column_sql_name}" <> '' ORDER BY "{column_sql_name}"'))
        return [r[0] for r in q.fetchall() if r[0] is not None]

# -----------------------------
# Sidebar 필터
# -----------------------------
st.sidebar.header("필터")

def_date_to = datetime.now()
def_date_from = def_date_to - timedelta(days=30)

from_date = st.sidebar.date_input("시작일", def_date_from)
end_date = st.sidebar.date_input("종료일", def_date_to)

platform_opts = [""] + distinct_values("플랫폼")
category_opts = [""] + distinct_values("유형")
locale_opts = [""] + distinct_values("로케일")

platform_f = st.sidebar.selectbox("플랫폼", platform_opts)
category_f = st.sidebar.selectbox("유형", category_opts)
locale_f = st.sidebar.selectbox("로케일", locale_opts)

q_text = st.sidebar.text_input("검색어(원인/영향/다음이력 포함)")

# -----------------------------
# 조회
# -----------------------------
st.title("장애 이력 관리 · 검색/추가/수정 (Streamlit)")

with SessionLocal() as s:
    conds = [Incident.started_at >= datetime.combine(from_date, datetime.min.time()),
             Incident.started_at < datetime.combine(end_date + timedelta(days=1), datetime.min.time())]

    if platform_f:
        conds.append(Incident.platform == platform_f)
    if category_f:
        conds.append(Incident.category == category_f)
    if locale_f:
        conds.append(Incident.locale == locale_f)
    if q_text:
        like = f"%{q_text}%"
        conds.append(or_(Incident.cause.ilike(like) if engine.url.get_backend_name().startswith("postgresql") else Incident.cause.like(like),
                         Incident.impact.ilike(like) if engine.url.get_backend_name().startswith("postgresql") else Incident.impact.like(like),
                         Incident.follow_up.ilike(like) if engine.url.get_backend_name().startswith("postgresql") else Incident.follow_up.like(like)))

    stmt = select(Incident).where(and_(*conds)).order_by(Incident.started_at.desc()).limit(500)
    rows = s.execute(stmt).scalars().all()

# 표 렌더링
import pandas as pd

def to_df(items: List[Incident]) -> pd.DataFrame:
    return pd.DataFrame([
        {
            "id": i.id,
            "발생일시_start": i.started_at,
            "발생일시_end": i.ended_at,
            "플랫폼": i.platform,
            "유형": i.category,
            "로케일": i.locale,
            "문의량": i.inquiries,
            "장애구분": i.incident_type,
            "원인": i.cause,
            "영향": i.impact,
            "다음이력": i.follow_up,
            "참고링크": i.ref_link,
            "비고": i.memo,
        }
        for i in items
    ])

st.subheader("조회 결과")
df = to_df(rows)
st.dataframe(df, use_container_width=True, height=360)

# -----------------------------
# 신규 추가
# -----------------------------
st.subheader("새 장애 추가")
with st.form("create_form", clear_on_submit=True):
    c1, c2, c3, c4 = st.columns(4)
    started_at = c1.datetime_input("발생일시_start", value=datetime.now())
    ended_at = c2.datetime_input("발생일시_end", value=None)
    platform_in = c3.selectbox("플랫폼", platform_opts[1:] + ["기타(직접입력)"])
    category_in = c4.selectbox("유형", category_opts[1:] + ["기타(직접입력)"])

    if platform_in == "기타(직접입력)":
        platform_in = st.text_input("플랫폼 직접입력", "")
    if category_in == "기타(직접입력)":
        category_in = st.text_input("유형 직접입력", "")

    c5, c6, c7 = st.columns(3)
    locale_in = c5.text_input("로케일 (예: KR/JP/ALL)")
    inquiries_in = c6.number_input("문의량", min_value=0, step=1)
    incident_type_in = c7.text_input("장애구분")

    cause_in = st.text_area("원인")
    impact_in = st.text_area("영향")
    follow_up_in = st.text_area("다음이력")
    ref_link_in = st.text_input("참고링크")
    memo_in = st.text_area("비고")

    submitted = st.form_submit_button("추가")
    if submitted:
        with SessionLocal() as s:
            item = Incident(
                started_at=started_at,
                ended_at=ended_at,
                platform=platform_in or None,
                category=category_in or None,
                locale=locale_in or None,
                inquiries=int(inquiries_in) if inquiries_in is not None else None,
                incident_type=incident_type_in or None,
                cause=cause_in or None,
                impact=impact_in or None,
                follow_up=follow_up_in or None,
                ref_link=ref_link_in or None,
                memo=memo_in or None,
            )
            s.add(item)
            try:
                s.commit()
                st.success("추가 완료")
            except Exception as e:
                s.rollback()
                st.error(f"추가 실패: {e}")

# -----------------------------
# 수정 (단일 레코드)
# -----------------------------
st.subheader("선택 항목 수정")

edit_id = st.number_input("수정할 id", min_value=0, step=1)
if edit_id:
    with SessionLocal() as s:
        item: Optional[Incident] = s.get(Incident, int(edit_id))
        if not item:
            st.info("해당 id 레코드가 없습니다.")
        else:
            with st.form("edit_form"):
                c1, c2, c3, c4 = st.columns(4)
                started_at_e = c1.datetime_input("발생일시_start", value=item.started_at)
                ended_at_e = c2.datetime_input("발생일시_end", value=item.ended_at)
                platform_e = c3.text_input("플랫폼", value=item.platform or "")
                category_e = c4.text_input("유형", value=item.category or "")

                c5, c6, c7 = st.columns(3)
                locale_e = c5.text_input("로케일", value=item.locale or "")
                inquiries_e = c6.number_input("문의량", min_value=0, step=1, value=int(item.inquiries or 0))
                incident_type_e = c7.text_input("장애구분", value=item.incident_type or "")

                cause_e = st.text_area("원인", value=item.cause or "")
                impact_e = st.text_area("영향", value=item.impact or "")
                follow_up_e = st.text_area("다음이력", value=item.follow_up or "")
                ref_link_e = st.text_input("참고링크", value=item.ref_link or "")
                memo_e = st.text_area("비고", value=item.memo or "")

                save = st.form_submit_button("저장")
                if save:
                    item.started_at = started_at_e
                    item.ended_at = ended_at_e
                    item.platform = platform_e or None
                    item.category = category_e or None
                    item.locale = locale_e or None
                    item.inquiries = int(inquiries_e) if inquiries_e is not None else None
                    item.incident_type = incident_type_e or None
                    item.cause = cause_e or None
                    item.impact = impact_e or None
                    item.follow_up = follow_up_e or None
                    item.ref_link = ref_link_e or None
                    item.memo = memo_e or None
                    try:
                        s.commit()
                        st.success("수정 완료")
                    except Exception as e:
                        s.rollback()
                        st.error(f"수정 실패: {e}")

# -----------------------------
# 삭제 (선택)
# -----------------------------
st.divider()
st.subheader("삭제")
with st.form("delete_form"):
    del_id = st.number_input("삭제할 id", min_value=0, step=1)
    do_delete = st.form_submit_button("삭제")
    if do_delete and del_id:
        with SessionLocal() as s:
            obj = s.get(Incident, int(del_id))
            if obj:
                try:
                    s.delete(obj)
                    s.commit()
                    st.warning(f"id={del_id} 삭제됨")
                except Exception as e:
                    s.rollback()
                    st.error(f"삭제 실패: {e}")
            else:
                st.info("해당 id 레코드가 없습니다.")

# -----------------------------
# 팁/보안
# -----------------------------
st.info(
    """
    • DB 접속 정보는 반드시 `.streamlit/secrets.toml` 또는 환경변수로 관리하세요.\n
    • 상위 환경(운영)에서는 Streamlit의 기본 인증이 없으므로, 프록시/아이피 제한/사내SSO 등으로 보호하세요.\n
    • 대량 적재/수정은 별도 배치/ETL에서 처리하고, 본 앱은 운영 CRUD/조회 위주로 사용을 권장합니다.
    """
)
