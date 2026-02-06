"""
SQLite 管理モジュール
工事データ（projects）の保存・取得を行う。
"""

import json
import sqlite3
from datetime import datetime
from pathlib import Path

# プロジェクトルートの db/ に migiude.db を配置
DB_DIR = Path(__file__).resolve().parent
DB_PATH = DB_DIR / "migiude.db"


def get_connection():
    """DB接続を返す。"""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    """データベースファイルと projects テーブルを作成する。"""
    DB_DIR.mkdir(parents=True, exist_ok=True)
    conn = get_connection()
    try:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS projects (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                project_type TEXT,
                project_code TEXT,
                corins_id TEXT,
                project_name TEXT,
                contract_amount INTEGER,
                start_date TEXT,
                end_date TEXT,
                location TEXT,
                client_name TEXT,
                contractor_name TEXT,
                field TEXT,
                work_types TEXT,
                engineers TEXT,
                summary TEXT,
                raw_json TEXT,
                folder_path TEXT,
                created_at TEXT
            )
        """)
        conn.commit()
        try:
            conn.execute("ALTER TABLE projects ADD COLUMN folder_path TEXT")
            conn.commit()
        except sqlite3.OperationalError as e:
            if "duplicate column name" not in str(e).lower():
                raise
        try:
            conn.execute("ALTER TABLE projects ADD COLUMN project_code TEXT")
            conn.commit()
        except sqlite3.OperationalError as e:
            if "duplicate column name" not in str(e).lower():
                raise
        conn.execute("""
            CREATE TABLE IF NOT EXISTS design_documents (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                project_id INTEGER,
                document_title TEXT,
                project_name TEXT,
                project_code TEXT,
                location TEXT,
                executing_office TEXT,
                contract_days INTEGER,
                budget_category TEXT,
                quantities TEXT,
                special_specs TEXT,
                raw_json TEXT,
                file_path TEXT,
                created_at TEXT,
                FOREIGN KEY (project_id) REFERENCES projects(id)
            )
        """)
        conn.commit()
    finally:
        conn.close()


def save_project(data: dict, project_type: str, sqlite_mode: str = "both") -> int:
    """
    工事データを1件保存する。

    Args:
        data: パーサー出力の辞書（corins_id, project_name, work_types, engineers など）
        project_type: 'past' または 'current'
        sqlite_mode: 'fixed' | 'json' | 'both' - 固定カラムのみ / JSONのみ / 両方

    Returns:
        挿入した行の id
    """
    raw_json = json.dumps(data, ensure_ascii=False)
    work_types_str = json.dumps(data.get("work_types") or [], ensure_ascii=False)
    engineers_str = json.dumps(data.get("engineers") or [], ensure_ascii=False)
    created_at = datetime.now().isoformat()

    conn = get_connection()
    try:
        if sqlite_mode == "json":
            # JSONのみ: 最小限の固定カラム + raw_json
            cur = conn.execute(
                """
                INSERT INTO projects (
                    project_type, project_code, folder_path, raw_json, created_at
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (
                    project_type,
                    data.get("project_code"),
                    data.get("folder_path"),
                    raw_json,
                    created_at,
                ),
            )
        elif sqlite_mode == "fixed":
            # 固定のみ: raw_jsonは空
            cur = conn.execute(
                """
                INSERT INTO projects (
                    project_type, project_code, corins_id, project_name, contract_amount,
                    start_date, end_date, location, client_name, contractor_name,
                    field, work_types, engineers, summary, raw_json, folder_path, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    project_type,
                    data.get("project_code"),
                    data.get("corins_id"),
                    data.get("project_name"),
                    data.get("contract_amount"),
                    data.get("start_date"),
                    data.get("end_date"),
                    data.get("location"),
                    data.get("client_name"),
                    data.get("contractor_name"),
                    data.get("field"),
                    work_types_str,
                    engineers_str,
                    data.get("summary"),
                    None,  # raw_json は保存しない
                    data.get("folder_path"),
                    created_at,
                ),
            )
        else:
            # both: 固定 + raw_json
            cur = conn.execute(
                """
                INSERT INTO projects (
                    project_type, project_code, corins_id, project_name, contract_amount,
                    start_date, end_date, location, client_name, contractor_name,
                    field, work_types, engineers, summary, raw_json, folder_path, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    project_type,
                    data.get("project_code"),
                    data.get("corins_id"),
                    data.get("project_name"),
                    data.get("contract_amount"),
                    data.get("start_date"),
                    data.get("end_date"),
                    data.get("location"),
                    data.get("client_name"),
                    data.get("contractor_name"),
                    data.get("field"),
                    work_types_str,
                    engineers_str,
                    data.get("summary"),
                    raw_json,
                    data.get("folder_path"),
                    created_at,
                ),
            )
        conn.commit()
        return cur.lastrowid
    finally:
        conn.close()


def get_project_count_by_year_prefix(prefix: str) -> int:
    """
    指定した年度prefixを持つ工事の件数を返す。

    Args:
        prefix: 年度prefix（例: "R4", "R5", "H29"）

    Returns:
        該当する工事の件数
    """
    conn = get_connection()
    try:
        cur = conn.execute(
            "SELECT COUNT(*) FROM projects WHERE project_code LIKE ?",
            (f"{prefix}-%",),
        )
        return cur.fetchone()[0]
    finally:
        conn.close()


def _row_to_dict(row: sqlite3.Row) -> dict:
    """sqlite3.Row を辞書に変換。work_types / engineers はリストに復元。JSONモード時はraw_jsonをベースに。"""
    d = dict(row)
    if d.get("work_types") is not None:
        try:
            d["work_types"] = json.loads(d["work_types"])
        except (json.JSONDecodeError, TypeError):
            d["work_types"] = []
    if d.get("engineers") is not None:
        try:
            d["engineers"] = json.loads(d["engineers"])
        except (json.JSONDecodeError, TypeError):
            d["engineers"] = []
    raw = d.get("raw_json")
    if raw is not None:
        try:
            parsed = json.loads(raw)
            if isinstance(parsed, dict):
                d["raw_json"] = parsed
                # JSONモード（固定カラムが空）の場合はraw_jsonをマージ
                if not d.get("project_name") and parsed:
                    for k, v in parsed.items():
                        if k not in ("id", "created_at") and d.get(k) is None:
                            d[k] = v
        except (json.JSONDecodeError, TypeError):
            pass
    return d


def get_all_projects() -> list[dict]:
    """全工事を取得する。"""
    conn = get_connection()
    try:
        cur = conn.execute(
            "SELECT * FROM projects ORDER BY id"
        )
        return [_row_to_dict(row) for row in cur.fetchall()]
    finally:
        conn.close()


def get_project_by_id(id: int) -> dict | None:
    """指定 id の工事を1件取得する。存在しなければ None。"""
    conn = get_connection()
    try:
        cur = conn.execute("SELECT * FROM projects WHERE id = ?", (id,))
        row = cur.fetchone()
        return _row_to_dict(row) if row else None
    finally:
        conn.close()


def delete_project(id: int) -> bool:
    """指定idの工事を削除する。成功したらTrue。"""
    conn = get_connection()
    try:
        conn.execute("DELETE FROM design_documents WHERE project_id = ?", (id,))
        cur = conn.execute("DELETE FROM projects WHERE id = ?", (id,))
        conn.commit()
        return cur.rowcount > 0
    finally:
        conn.close()


def save_design_document(data: dict, project_id: int | None = None) -> int:
    """設計図書データを1件保存する。"""
    raw_json = json.dumps(data, ensure_ascii=False)
    quantities_str = json.dumps(data.get("quantities") or [], ensure_ascii=False)
    created_at = datetime.now().isoformat()
    conn = get_connection()
    try:
        cur = conn.execute(
            """
            INSERT INTO design_documents (
                project_id, document_title, project_name, project_code,
                location, executing_office, contract_days, budget_category,
                quantities, special_specs, raw_json, file_path, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                project_id,
                data.get("document_title"),
                data.get("project_name"),
                data.get("project_code"),
                data.get("location"),
                data.get("executing_office"),
                data.get("contract_days"),
                data.get("budget_category"),
                quantities_str,
                data.get("special_specs"),
                raw_json,
                data.get("file_path"),
                created_at,
            ),
        )
        conn.commit()
        return cur.lastrowid
    finally:
        conn.close()


def get_design_documents_by_project(project_id: int) -> list[dict]:
    """指定工事の設計図書を全件取得する。"""
    conn = get_connection()
    try:
        cur = conn.execute(
            "SELECT * FROM design_documents WHERE project_id = ? ORDER BY id",
            (project_id,),
        )
        return [dict(row) for row in cur.fetchall()]
    finally:
        conn.close()


def get_all_design_documents() -> list[dict]:
    """全設計図書を取得する。"""
    conn = get_connection()
    try:
        cur = conn.execute("SELECT * FROM design_documents ORDER BY id")
        return [dict(row) for row in cur.fetchall()]
    finally:
        conn.close()


def delete_design_document(id: int) -> bool:
    """設計図書を1件削除する。"""
    conn = get_connection()
    try:
        cur = conn.execute("DELETE FROM design_documents WHERE id = ?", (id,))
        conn.commit()
        return cur.rowcount > 0
    finally:
        conn.close()
