"""
Migiude 工事登録
過去工事と稼働中の工事をコリンズPDFで登録する。
"""

import os
import shutil
import tempfile
from pathlib import Path

import streamlit as st

from db.sqlite_manager import (
    delete_project,
    get_all_projects,
    init_db,
    get_design_documents_by_project,
    get_project_by_id,
    save_design_document,
)
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
        # ===== 1. コリンズ登録 =====
        st.markdown("### コリンズ登録")
        uploaded_file = st.file_uploader(
            "コリンズ（PDF/MD）をアップロード",
            type=["pdf", "md"],
            key=f"upload_{project_type}",
        )
        if st.button("工事を登録", key=f"btn_register_{project_type}"):
            if uploaded_file:
                ext = (
                    ".pdf"
                    if uploaded_file.name.lower().endswith(".pdf")
                    else ".md"
                )
                with tempfile.NamedTemporaryFile(
                    delete=False,
                    suffix=ext,
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
                st.error("ファイルをアップロードしてください")

        st.markdown("---")

        # ===== 2. 設計図書登録 =====
        projects = [
            p for p in get_all_projects() if p.get("project_type") == project_type
        ]

        if projects:
            st.markdown("### 設計図書登録")
            project_options = {
                f"{p.get('project_code', '')} {p['project_name']}": p["id"]
                for p in projects
            }
            selected_project = st.selectbox(
                "紐付ける工事",
                options=list(project_options.keys()),
                key=f"design_project_{project_type}",
            )
            design_file = st.file_uploader(
                "設計図書PDFをアップロード",
                type=["pdf"],
                key=f"upload_design_{project_type}",
            )
            if st.button("設計図書を登録", key=f"btn_design_{project_type}"):
                if design_file:
                    with tempfile.NamedTemporaryFile(
                        delete=False, suffix=".pdf"
                    ) as tmp:
                        tmp.write(design_file.read())
                        tmp_path = tmp.name
                    try:
                        from parsers.design_doc_parser import (
                            parse_design_doc_pdf,
                        )

                        design_data = parse_design_doc_pdf(tmp_path)
                        project_id = project_options[selected_project]
                        project = get_project_by_id(project_id)
                        if project and project.get("folder_path"):
                            project_root = Path(__file__).resolve().parent
                            design_dir = (
                                project_root
                                / project["folder_path"].replace("/", os.sep)
                                / "設計書"
                            )
                            design_dir.mkdir(parents=True, exist_ok=True)
                            dest = design_dir / design_file.name
                            if dest.exists():
                                base, ext = dest.stem, dest.suffix
                                n = 2
                                while (design_dir / f"{base}_{n}{ext}").exists():
                                    n += 1
                                dest = design_dir / f"{base}_{n}{ext}"
                            shutil.copy2(tmp_path, dest)
                            design_data["file_path"] = str(
                                dest.relative_to(project_root)
                            ).replace("\\", "/")
                        save_design_document(design_data, project_id)
                        st.success(
                            f"登録完了: {design_data.get('document_title', '設計図書')}"
                        )
                        st.rerun()
                    except Exception as e:
                        st.error(f"エラー: {e}")
                    finally:
                        os.unlink(tmp_path)
                else:
                    st.error("PDFをアップロードしてください")

            st.markdown("---")

        # ===== 3. 登録済み工事一覧 =====
        st.markdown("### 登録済み工事一覧")

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
                col1, col2 = st.columns([3, 1])
                with col1:
                    code = p.get("project_code") or ""
                    amount = (
                        f"{p['contract_amount']:,}円"
                        if p.get("contract_amount")
                        else "-"
                    )
                    st.write(f"**{code}** {p['project_name']} - {amount}")

                    # DB紐付け状況を表示
                    corins_db = get_db_selection("コリンズ")
                    corins_dbs = [
                        name
                        for name, key in [
                            ("SQLite", "sqlite"),
                            ("Chroma", "chroma"),
                            ("Neo4j", "neo4j"),
                        ]
                        if corins_db.get(key)
                    ]
                    corins_db_str = (
                        f" [{', '.join(corins_dbs)}]" if corins_dbs else ""
                    )
                    corins_mark = f"✅コリンズ{corins_db_str}"

                    design_docs = get_design_documents_by_project(p["id"])
                    if design_docs:
                        design_db = get_db_selection("設計図書")
                        design_dbs = [
                            name
                            for name, key in [
                                ("SQLite", "sqlite"),
                                ("Chroma", "chroma"),
                                ("Neo4j", "neo4j"),
                            ]
                            if design_db.get(key)
                        ]
                        design_db_str = (
                            f" [{', '.join(design_dbs)}]"
                            if design_dbs
                            else ""
                        )
                        design_mark = f"✅設計図書{design_db_str}"
                    else:
                        design_mark = "❌設計図書"

                    st.caption(f"{corins_mark}　{design_mark}")

                with col2:
                    if st.button("削除", key=f"del_{project_type}_{p['id']}"):
                        delete_project(p["id"])
                        if (get_db_selection() or {}).get("neo4j"):
                            delete_project_from_neo4j(p["id"])
                        st.rerun()

with tab_settings:
    st.markdown("### 設定")
