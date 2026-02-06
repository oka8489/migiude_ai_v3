"""
Migiude 工事登録
過去工事と稼働中の工事をコリンズPDFで登録する。
"""

import os
import tempfile

import streamlit as st

from db.sqlite_manager import delete_project, get_all_projects, init_db
from services.neo4j_service import delete_project_from_neo4j
from services.project_service import register_project_from_corins
from services.config_service import get_db_selection

st.set_page_config(page_title="Migiude 工事登録", layout="wide")

init_db()

# ========== 工事登録 ==========
st.markdown("# 工事登録")

tab_current, tab_past, tab_settings = st.tabs(["稼働中の工事", "過去工事", "設定"])

for project_type, tab in [("current", tab_current), ("past", tab_past)]:
    with tab:
        st.markdown("### コリンズPDFをアップロード")
        uploaded_file = st.file_uploader(
            "コリンズPDFをアップロード",
            type=["pdf"],
            key=f"upload_{project_type}",
        )
        if st.button("登録", key=f"btn_register_{project_type}"):
            if uploaded_file:
                with tempfile.NamedTemporaryFile(
                    delete=False,
                    suffix=".pdf",
                ) as tmp:
                    tmp.write(uploaded_file.read())
                    tmp_path = tmp.name
                try:
                    result = register_project_from_corins(tmp_path, project_type)
                    st.success(
                        f"登録完了: {result.get('project_code', '')} {result['project_name']}"
                    )
                    st.rerun()
                except Exception as e:
                    st.error(f"エラー: {e}")
                finally:
                    os.unlink(tmp_path)
            else:
                st.error("PDFをアップロードしてください")

        st.markdown("---")
        st.markdown("### 登録済み一覧")

        projects = [p for p in get_all_projects() if p.get("project_type") == project_type]
        if not projects:
            st.info("工事が登録されていません")
        else:
            if st.button("全件削除", key=f"del_all_{project_type}"):
                db_sel = get_db_selection() or {}
                for p in projects:
                    delete_project(p["id"])
                    if db_sel.get("neo4j"):
                        delete_project_from_neo4j(p["id"])
                st.rerun()

            for p in projects:
                col1, col2 = st.columns([4, 1])
                with col1:
                    code = p.get("project_code") or ""
                    amount = (
                        f"{p['contract_amount']:,}円"
                        if p.get("contract_amount")
                        else "-"
                    )
                    st.write(f"**{code}** {p['project_name']} - {amount}")
                with col2:
                    if st.button("削除", key=f"del_{project_type}_{p['id']}"):
                        delete_project(p["id"])
                        if (get_db_selection() or {}).get("neo4j"):
                            delete_project_from_neo4j(p["id"])
                        st.rerun()

with tab_settings:
    st.markdown("### 設定")
