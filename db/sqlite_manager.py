"""
SQLite 管理モジュール
工事データ（projects）の保存・取得を行う。
"""

import json
import os
import shutil

from pathlib import Path

from dotenv import load_dotenv

# プロジェクトルートの .env を確実に読み込む
load_dotenv(Path(__file__).resolve().parent.parent / ".env")

import sqlite3
from datetime import datetime

# DBパス: .env の MIGIUDE_DB_PATH があればそれを使用（パス問題対策）
_project_root = Path(__file__).resolve().parent.parent
_default_db = _project_root / "db" / "migiude.db"
_env_path = os.environ.get("MIGIUDE_DB_PATH")
if _env_path:
    DB_PATH = Path(_env_path)
else:
    DB_PATH = _default_db
DB_DIR = DB_PATH.parent
DB_PATH = DB_PATH.resolve()  # 絶対パスに統一


def get_db_path() -> str:
    """DBファイルの絶対パスを返す（デバッグ・確認用）。"""
    return str(DB_PATH.resolve())


def get_connection():
    """DB接続を返す。"""
    conn = sqlite3.connect(str(DB_PATH))
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
        try:
            conn.execute(
                "ALTER TABLE projects ADD COLUMN saved_to_neo4j INTEGER DEFAULT 0"
            )
            conn.commit()
        except sqlite3.OperationalError as e:
            if "duplicate column name" not in str(e).lower():
                raise
        try:
            conn.execute(
                "ALTER TABLE projects ADD COLUMN saved_to_chroma INTEGER DEFAULT 0"
            )
            conn.commit()
        except sqlite3.OperationalError as e:
            if "duplicate column name" not in str(e).lower():
                raise
        conn.execute("""
            CREATE TABLE IF NOT EXISTS kb_folders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.commit()
        conn.execute("""
            CREATE TABLE IF NOT EXISTS kb_documents (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                doc_id INTEGER UNIQUE,
                folder_id INTEGER,
                title TEXT NOT NULL,
                source_type TEXT NOT NULL,
                total_chunks INTEGER DEFAULT 0,
                page_count INTEGER DEFAULT 0,
                toc TEXT,
                source_url TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (folder_id) REFERENCES kb_folders(id)
            )
        """)
        conn.commit()
        # マイグレーション: Chromaのフォルダ別コレクション(kb_N)をkb_documentsに反映
        try:
            cur = conn.execute("SELECT COUNT(*) FROM kb_documents")
            if cur.fetchone()[0] == 0:
                try:
                    from services.chroma_service import _get_client
                    client = _get_client()
                    import re
                    for col_obj in client.list_collections():
                        name = col_obj.name
                        m = re.match(r"^kb_(\d+)$", name)
                        if not m:
                            continue
                        folder_id = int(m.group(1))
                        col = client.get_collection(name=name)
                        all_data = col.get(include=["metadatas"], limit=10000)
                        metas = all_data.get("metadatas") or []
                        if metas and isinstance(metas[0], list):
                            metas = [m for sub in metas for m in (sub or [])]
                        if metas:
                            fc = conn.execute("SELECT id FROM kb_folders WHERE id = ?", (folder_id,))
                            if not fc.fetchone():
                                conn.execute("INSERT INTO kb_folders (id, name) VALUES (?, ?)", (folder_id, f"フォルダ{folder_id}"))
                            seen = set()
                            for meta in metas:
                                if not meta:
                                    continue
                                doc_id = meta.get("doc_id")
                                if not doc_id or doc_id in seen:
                                    continue
                                seen.add(doc_id)
                                conn.execute(
                                    "INSERT OR IGNORE INTO kb_documents (doc_id, folder_id, title, source_type, total_chunks) VALUES (?, ?, ?, ?, ?)",
                                    (doc_id, folder_id, meta.get("source_title", "不明"), meta.get("source_type", ""), meta.get("total_chunks", 0)),
                                )
                    conn.commit()
                except Exception:
                    pass
        except Exception:
            pass
        conn.execute("""
            CREATE TABLE IF NOT EXISTS migiude_rules (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                category TEXT NOT NULL,
                rule_name TEXT NOT NULL,
                rule_content TEXT NOT NULL,
                is_active INTEGER DEFAULT 1,
                priority INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.commit()
        # design_documents テーブルは不要（設計図書はChromaのみに保存）
        try:
            conn.execute("DROP TABLE IF EXISTS design_documents")
            conn.commit()
        except sqlite3.OperationalError:
            pass
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
        pk = cur.lastrowid
        # 即時反映確認: 保存直後に同一DBから件数を取得・ログ出力
        _verify = conn.execute("SELECT COUNT(*) FROM projects").fetchone()[0]
        _log_path = DB_DIR / "sqlite_debug.log"
        with open(_log_path, "a", encoding="utf-8") as f:
            from datetime import datetime as dt
            f.write(f"{dt.now().isoformat()} | DB={DB_PATH} | saved_id={pk} | total_count={_verify}\n")
        return pk
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


def update_project_saved_to_db(project_id: int, saved_to_neo4j: bool | None = None, saved_to_chroma: bool | None = None) -> None:
    """工事のDB保存フラグを更新する。"""
    updates = []
    params = []
    if saved_to_neo4j is not None:
        updates.append("saved_to_neo4j = ?")
        params.append(1 if saved_to_neo4j else 0)
    if saved_to_chroma is not None:
        updates.append("saved_to_chroma = ?")
        params.append(1 if saved_to_chroma else 0)
    if not updates:
        return
    params.append(project_id)
    conn = get_connection()
    try:
        conn.execute(
            f"UPDATE projects SET {', '.join(updates)} WHERE id = ?",
            params,
        )
        conn.commit()
    finally:
        conn.close()


def delete_project(id: int) -> bool:
    """指定idの工事を削除する。関連フォルダも削除。"""
    conn = get_connection()
    try:
        # 先にfolder_pathを取得
        cur = conn.execute(
            "SELECT folder_path FROM projects WHERE id = ?", (id,)
        )
        row = cur.fetchone()
        folder_path = row["folder_path"] if row else None

        # DB削除
        cur = conn.execute("DELETE FROM projects WHERE id = ?", (id,))
        conn.commit()
        deleted = cur.rowcount > 0

        # フォルダ削除
        if deleted and folder_path:
            full_path = DB_DIR.parent / folder_path
            if full_path.exists():
                shutil.rmtree(full_path)

        return deleted
    finally:
        conn.close()


def clear_kb_data() -> None:
    """kb_documents と kb_folders をクリアする（ライブラリを空にする）。"""
    conn = get_connection()
    try:
        conn.execute("DELETE FROM kb_documents")
        conn.execute("DELETE FROM kb_folders")
        conn.commit()
    finally:
        conn.close()


# ========== migiude_rules（mmモードルール） ==========


def get_migiude_rules(category: str | None = None) -> list[dict]:
    """migiude_rulesを取得。category指定時はフィルタ。"""
    conn = get_connection()
    try:
        if category:
            cur = conn.execute(
                "SELECT * FROM migiude_rules WHERE category = ? ORDER BY priority DESC, id",
                (category,),
            )
        else:
            cur = conn.execute(
                "SELECT * FROM migiude_rules ORDER BY priority DESC, category, id"
            )
        rows = cur.fetchall()
        return [dict(row) for row in rows]
    finally:
        conn.close()


def get_migiude_rules_categories() -> list[str]:
    """登録済みカテゴリ一覧を取得。"""
    conn = get_connection()
    try:
        cur = conn.execute(
            "SELECT DISTINCT category FROM migiude_rules ORDER BY category"
        )
        return [row[0] for row in cur.fetchall()]
    finally:
        conn.close()


def save_migiude_rule(
    category: str,
    rule_name: str,
    rule_content: str,
    priority: int = 0,
    is_active: int = 1,
    rule_id: int | None = None,
) -> int:
    """migiude_ruleを保存。rule_id指定時は更新。"""
    now = datetime.now().isoformat()
    conn = get_connection()
    try:
        if rule_id:
            conn.execute(
                """UPDATE migiude_rules SET
                    category=?, rule_name=?, rule_content=?, is_active=?, priority=?,
                    updated_at=?
                WHERE id=?""",
                (category, rule_name, rule_content, is_active, priority, now, rule_id),
            )
            conn.commit()
            return rule_id
        else:
            cur = conn.execute(
                """INSERT INTO migiude_rules (category, rule_name, rule_content, is_active, priority)
                VALUES (?, ?, ?, ?, ?)""",
                (category, rule_name, rule_content, is_active, priority),
            )
            conn.commit()
            return cur.lastrowid
    finally:
        conn.close()


def update_migiude_rule_active(rule_id: int, is_active: bool) -> None:
    """ルールの有効/無効を切り替え。"""
    conn = get_connection()
    try:
        conn.execute(
            "UPDATE migiude_rules SET is_active=?, updated_at=? WHERE id=?",
            (1 if is_active else 0, datetime.now().isoformat(), rule_id),
        )
        conn.commit()
    finally:
        conn.close()


def delete_migiude_rule(rule_id: int) -> bool:
    """migiude_ruleを削除。"""
    conn = get_connection()
    try:
        cur = conn.execute("DELETE FROM migiude_rules WHERE id = ?", (rule_id,))
        conn.commit()
        return cur.rowcount > 0
    finally:
        conn.close()


def count_migiude_rules() -> int:
    """migiude_rulesの件数を返す。"""
    conn = get_connection()
    try:
        cur = conn.execute("SELECT COUNT(*) FROM migiude_rules")
        return cur.fetchone()[0]
    finally:
        conn.close()


def insert_initial_migiude_rules() -> None:
    """テーブルが空の場合、初期ルールを投入。"""
    if count_migiude_rules() > 0:
        return
    rules = [
        (
            "日報作成",
            "日報インタビューフロー",
            "インタビュー形式で日報を作成。回答中に経験・判断・工夫を検知したら深掘りする。最後に『覚えておきましょうか？』と確認し、OKならChromaのtacit_knowledge（category/tags/project_code付き）に保存する。",
            0,
        ),
        (
            "DB検索",
            "情報不足時の対応",
            "検索して情報がない場合、『ありません』で終わらず、その情報を得るために必要なドキュメントや行動を提案する。例：『この情報は○○の書類に記載されています。アップロードしますか？』",
            0,
        ),
        (
            "応答スタイル",
            "基本応答ルール",
            "建設業の現場所長に話すようなトーンで応答する。専門用語はそのまま使い、過度な説明は省く。",
            0,
        ),
    ]
    conn = get_connection()
    try:
        for category, rule_name, rule_content, priority in rules:
            conn.execute(
                """INSERT INTO migiude_rules (category, rule_name, rule_content, is_active, priority)
                VALUES (?, ?, ?, ?, ?)""",
                (category, rule_name, rule_content, 1, priority),
            )
        conn.commit()
    finally:
        conn.close()


