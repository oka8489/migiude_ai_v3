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
        # document_skills（書類Skill定義）
        conn.execute("""
            CREATE TABLE IF NOT EXISTS document_skills (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                skill_name TEXT NOT NULL,
                description TEXT,
                input_mode TEXT NOT NULL
                    CHECK(input_mode IN ('extract', 'text')),
                skill_md_path TEXT,
                template_path TEXT,
                sample_path TEXT,
                output_format TEXT DEFAULT 'xlsx'
                    CHECK(output_format IN ('xlsx', 'docx', 'pdf')),
                use_vision INTEGER DEFAULT 0,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.commit()
        # マイグレーション: document_skills の input_mode に 'text' を追加
        try:
            cur = conn.execute(
                "SELECT sql FROM sqlite_master WHERE type='table' AND name='document_skills'"
            )
            row = cur.fetchone()
            if row and row[0] and "'text'" not in row[0] or "'hearing'" in row[0]:
                # 旧スキーマ（text なし）→ テーブル再作成
                conn.execute("PRAGMA foreign_keys=OFF")
                conn.execute("""
                    CREATE TABLE document_skills_new (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        skill_name TEXT NOT NULL,
                        description TEXT,
                        input_mode TEXT NOT NULL
                            CHECK(input_mode IN ('extract', 'text')),
                        skill_md_path TEXT,
                        template_path TEXT,
                        sample_path TEXT,
                        output_format TEXT DEFAULT 'xlsx'
                            CHECK(output_format IN ('xlsx', 'docx', 'pdf')),
                        use_vision INTEGER DEFAULT 0,
                        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                conn.execute("""
                    INSERT INTO document_skills_new
                    SELECT id, skill_name, description,
                        CASE WHEN input_mode='hearing' THEN 'text' ELSE input_mode END,
                        skill_md_path, template_path, sample_path, output_format, use_vision,
                        created_at, updated_at
                    FROM document_skills
                """)
                conn.execute("DROP TABLE document_skills")
                conn.execute("ALTER TABLE document_skills_new RENAME TO document_skills")
                conn.execute("PRAGMA foreign_keys=ON")
                conn.commit()
        except sqlite3.OperationalError:
            conn.execute("PRAGMA foreign_keys=ON")
            conn.rollback()
        # skill_required_sources（必要なデータ元書類）
        conn.execute("""
            CREATE TABLE IF NOT EXISTS skill_required_sources (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                skill_id INTEGER NOT NULL,
                source_name TEXT NOT NULL,
                source_type TEXT NOT NULL
                    CHECK(source_type IN ('pdf', 'xlsx', 'xls', 'image', 'db_query')),
                source_description TEXT,
                source_path TEXT,
                is_required INTEGER DEFAULT 1,
                db_table TEXT,
                db_query_template TEXT,
                FOREIGN KEY (skill_id) REFERENCES document_skills(id) ON DELETE CASCADE
            )
        """)
        conn.commit()
        # skill_field_mappings（フィールドマッピング）
        conn.execute("""
            CREATE TABLE IF NOT EXISTS skill_field_mappings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                skill_id INTEGER NOT NULL,
                field_name TEXT NOT NULL,
                cell_reference TEXT,
                source_id INTEGER,
                extraction_hint TEXT,
                default_value TEXT,
                FOREIGN KEY (skill_id) REFERENCES document_skills(id) ON DELETE CASCADE,
                FOREIGN KEY (source_id) REFERENCES skill_required_sources(id) ON DELETE SET NULL
            )
        """)
        conn.commit()
        # design_documents テーブルは不要（設計図書はChromaのみに保存）
        try:
            conn.execute("DROP TABLE IF EXISTS design_documents")
            conn.commit()
        except sqlite3.OperationalError:
            pass
        # migiude_rules はClaude Desktopのスキルで管理するため不要
        try:
            conn.execute("DROP TABLE IF EXISTS migiude_rules")
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


# ========== document_skills（書類Skill） ==========


def _parse_mappings_from_skill_md(skill_md: str) -> list[dict]:
    """SKILL.mdからマッピングルールをパースする。"""
    import re
    mappings = []
    # 「マッピングルール」セクションのテーブルを探す
    match = re.search(r"## マッピングルール.*?\n([\s\S]*?)(?=\n## |\Z)", skill_md)
    if not match:
        return mappings
    table = match.group(1)
    lines = table.strip().split("\n")
    if len(lines) < 2:
        return mappings
    # ヘッダー行をスキップ（| JSONキー | → | セル位置 | 変換ルール |）
    for line in lines[2:]:
        if "|" not in line:
            continue
        cells = [c.strip() for c in line.split("|") if c.strip()]
        if len(cells) >= 3:
            field_name = cells[0]  # JSONキー
            cell_ref = cells[2] if len(cells) > 2 else ""  # セル位置
            hint = cells[3] if len(cells) > 3 else ""  # 変換ルール/抽出ヒント
            if field_name and not field_name.startswith("---"):
                mappings.append({
                    "field_name": field_name,
                    "cell_reference": cell_ref if "!" in cell_ref or re.match(r"[A-Z]+\d+", cell_ref) else "",
                    "extraction_hint": hint,
                })
    return mappings


def register_skill_to_db(
    skill_name: str,
    description: str,
    input_mode: str,
    use_vision: bool = False,
) -> int:
    """
    save_skill_package()の後に呼ぶ。SQLiteにSkill情報を登録する。
    既存の場合は更新する。
    """
    skill_dir_rel = f"skills/{skill_name}"
    skill_dir_full = _project_root / "skills" / skill_name

    if not skill_dir_full.exists():
        raise FileNotFoundError(f"スキルディレクトリが存在しません: {skill_dir_rel}")

    # テンプレート・サンプルのパスを検出（拡張子は .xlsx または .xls）
    template_path = ""
    sample_path = ""
    for ext in [".xlsx", ".xls"]:
        bp = skill_dir_full / f"template_blank{ext}"
        fp = skill_dir_full / f"template_filled{ext}"
        if bp.exists():
            template_path = f"{skill_dir_rel}/template_blank{ext}"
        if fp.exists():
            sample_path = f"{skill_dir_rel}/template_filled{ext}"
        if template_path and sample_path:
            break

    if not template_path:
        template_path = f"{skill_dir_rel}/template_blank.xlsx"
    if not sample_path:
        sample_path = f"{skill_dir_rel}/template_filled.xlsx"

    skill_md_path = f"{skill_dir_rel}/SKILL.md"
    skill_md_full = skill_dir_full / "SKILL.md"

    conn = get_connection()
    try:
        existing = conn.execute(
            "SELECT id FROM document_skills WHERE skill_name = ?", (skill_name,)
        ).fetchone()

        now = datetime.now().isoformat()

        if existing:
            skill_id = existing[0]
            conn.execute(
                """UPDATE document_skills SET
                    description=?, input_mode=?, skill_md_path=?, template_path=?,
                    sample_path=?, use_vision=?, updated_at=?
                WHERE id=?""",
                (description, input_mode, skill_md_path, template_path, sample_path,
                 1 if use_vision else 0, now, skill_id),
            )
            conn.execute("DELETE FROM skill_required_sources WHERE skill_id = ?", (skill_id,))
            conn.execute("DELETE FROM skill_field_mappings WHERE skill_id = ?", (skill_id,))
        else:
            cur = conn.execute(
                """INSERT INTO document_skills
                (skill_name, description, input_mode, skill_md_path, template_path, sample_path, use_vision)
                VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (skill_name, description, input_mode, skill_md_path, template_path, sample_path,
                 1 if use_vision else 0),
            )
            skill_id = cur.lastrowid

        conn.commit()

        # skill_required_sources（Phase 1: 1ファイルのみ）
        source_path = ""
        src_type = "pdf"
        for ext in [".pdf", ".xlsx", ".xls", ".md", ".txt"]:
            sp = skill_dir_full / f"source_sample{ext}"
            if sp.exists():
                source_path = f"{skill_dir_rel}/source_sample{ext}"
                raw = ext.lstrip(".").lower()
                src_type = "xls" if raw == "xls" else ("xlsx" if raw in ("xlsx", "xlsm") else "pdf")
                break

        if source_path:
            conn.execute(
                """INSERT INTO skill_required_sources
                (skill_id, source_name, source_type, source_path, is_required)
                VALUES (?, ?, ?, ?, ?)""",
                (skill_id, "データ元書類", src_type, source_path, 1),
            )
            conn.commit()

        # skill_field_mappings（SKILL.mdからパース）
        if skill_md_full.exists():
            with open(skill_md_full, "r", encoding="utf-8") as f:
                skill_md = f.read()
            mappings = _parse_mappings_from_skill_md(skill_md)
            for m in mappings:
                conn.execute(
                    """INSERT INTO skill_field_mappings
                    (skill_id, field_name, cell_reference, extraction_hint)
                    VALUES (?, ?, ?, ?)""",
                    (skill_id, m["field_name"], m.get("cell_reference") or "", m.get("extraction_hint") or ""),
                )
            conn.commit()

        return skill_id
    finally:
        conn.close()


def get_document_skills(skill_name_pattern: str | None = None) -> list[dict]:
    """document_skills を取得。skill_name_pattern 指定時は LIKE で絞り込み。"""
    conn = get_connection()
    try:
        if skill_name_pattern:
            cur = conn.execute(
                "SELECT * FROM document_skills WHERE skill_name LIKE ? ORDER BY id",
                (f"%{skill_name_pattern}%",),
            )
        else:
            cur = conn.execute("SELECT * FROM document_skills ORDER BY id")
        return [dict(row) for row in cur.fetchall()]
    finally:
        conn.close()


