"""
工事登録サービス
コリンズPDFからパースしてSQLiteに保存する。DB選択に応じてNeo4jにも保存。
"""

import re
import shutil
from pathlib import Path

from parsers.corins_parser import parse_corins_file
from db.sqlite_manager import init_db, save_project, get_project_by_id, get_project_count_by_year_prefix

try:
    from services.config_service import get_db_selection
    from services.neo4j_service import save_project_to_neo4j
except ImportError:
    get_db_selection = None
    save_project_to_neo4j = None

# プロジェクトルート（services/ の親）
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"


def _sanitize_folder_name(name: str) -> str:
    """フォルダ名に使えない文字を _ に置換する。"""
    return re.sub(r'[\\/:*?"<>|]', "_", name)


def _extract_year_prefix(project_name: str) -> str:
    """
    工事名から年度prefixを抽出する。

    Args:
        project_name: 工事名（例: "令和４年度 防安国舗日 第１－２号 舗装補修工事"）

    Returns:
        年度prefix（例: "R4", "R5", "H29"）。見つからない場合は "XX"
    """
    reiwa_match = re.search(r'令和([０-９0-9]+)年度', project_name)
    if reiwa_match:
        year_str = reiwa_match.group(1)
        year_str = year_str.translate(str.maketrans('０１２３４５６７８９', '0123456789'))
        return f"R{year_str}"

    heisei_match = re.search(r'平成([０-９0-9]+)年度', project_name)
    if heisei_match:
        year_str = heisei_match.group(1)
        year_str = year_str.translate(str.maketrans('０１２３４５６７８９', '0123456789'))
        return f"H{year_str}"

    return "XX"


def _generate_project_code(project_name: str) -> str:
    """
    工事番号を生成する。

    Args:
        project_name: 工事名

    Returns:
        工事番号（例: "R4-01", "R5-03"）
    """
    prefix = _extract_year_prefix(project_name)
    count = get_project_count_by_year_prefix(prefix)
    seq = count + 1
    return f"{prefix}-{seq:02d}"


def register_project_from_corins(
    file_path: str, project_type: str, save_to_neo4j: bool | None = None
) -> dict:
    """
    コリンズファイル（PDF/MD）から工事を登録する。

    Args:
        file_path: コリンズPDFまたはMDのパス
        project_type: 'past' または 'current'
        save_to_neo4j: Neo4jに保存するか。Noneの場合は設定に従う。Falseで明示的にスキップ。

    Returns:
        保存したデータ（idを含む、project_code, folder_pathを含む）
    """
    data = parse_corins_file(file_path)

    project_name = data.get("project_name") or "未命名"
    project_code = _generate_project_code(project_name)
    data["project_code"] = project_code

    safe_name = _sanitize_folder_name(project_name)
    folder_name = f"{project_code}_{safe_name}"
    if project_type == "past":
        folder = DATA_DIR / "過去工事" / folder_name
    else:
        folder = DATA_DIR / "新規工事" / folder_name
    folder.mkdir(parents=True, exist_ok=True)
    ext = Path(file_path).suffix.lower()
    dest_name = "コリンズ.pdf" if ext == ".pdf" else "コリンズ.md"
    dest_file = folder / dest_name
    shutil.copy2(file_path, dest_file)
    folder_path = str(folder.relative_to(PROJECT_ROOT)).replace("\\", "/")
    data["folder_path"] = folder_path

    init_db()

    db_sel = get_db_selection() if get_db_selection else {}
    save_to_sqlite = db_sel.get("sqlite", True)
    sqlite_mode = db_sel.get("sqlite_mode", "both")

    # 工事登録はSQLiteを必須とする（プロジェクトID・設計図書紐付けのため）
    if not save_to_sqlite:
        raise ValueError(
            "工事登録にはSQLiteが必要です。設定でSQLiteを有効にしてください。"
        )

    pk = save_project(data, project_type, sqlite_mode)
    saved = get_project_by_id(pk)

    # Neo4jに保存（save_to_neo4j が None の場合は設定に従う。False の場合はスキップ）
    if saved and save_project_to_neo4j:
        if save_to_neo4j is True:
            save_project_to_neo4j(saved)
        elif save_to_neo4j is None and get_db_selection:
            if get_db_selection().get("neo4j"):
                save_project_to_neo4j(saved)

    return saved


def register_all_corins_in_folder(folder_path: str, project_type: str) -> list[dict]:
    """
    指定フォルダ内の「コリンズ」を含むPDFを全て登録する。

    Args:
        folder_path: フォルダのパス（例: 'data'）
        project_type: 'past' または 'current'

    Returns:
        登録したデータのリスト
    """
    folder = Path(folder_path)
    pdf_files = list(folder.rglob("*コリンズ*.pdf"))

    results = []
    for pdf_path in pdf_files:
        result = register_project_from_corins(str(pdf_path), project_type)
        results.append(result)

    return results
