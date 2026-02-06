"""
設定サービス
データソース（スキーマ・パーサー）をJSONで管理する。
汎用的にコリンズ・設計書・見積書など複数のデータソースを登録可能。
"""

import json
import uuid
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_SOURCES_PATH = PROJECT_ROOT / "config" / "data_sources.json"

# コリンズ用デフォルトスキーマ（初回・フォールバック用）
DEFAULT_CORINS_SCHEMA = [
    {"key": "corins_id", "type": "string", "description": "登録番号"},
    {"key": "project_name", "type": "string", "description": "件名"},
    {"key": "contract_amount", "type": "number", "description": "請負金額（数値のみ。カンマや円は除く。不明ならnull）"},
    {"key": "start_date", "type": "date", "description": "工期開始（YYYY-MM-DD形式。不明ならnull）"},
    {"key": "end_date", "type": "date", "description": "工期終了（YYYY-MM-DD形式。不明ならnull）"},
    {"key": "location", "type": "string", "description": "施工場所"},
    {"key": "client_name", "type": "string", "description": "発注機関名"},
    {"key": "contractor_name", "type": "string", "description": "請負者名称"},
    {"key": "field", "type": "string", "description": "公共事業の分野"},
    {"key": "work_types", "type": "array", "description": "工種（文字列の配列）"},
    {"key": "engineers", "type": "array", "description": "技術者（配列。各要素は {\"name\": \"氏名\", \"role\": \"役割\"} の形式）"},
    {"key": "summary", "type": "string", "description": "工事概要（あれば文字列、なければnull）"},
]

# 設計図書用デフォルトスキーマ
DEFAULT_DESIGN_DOC_SCHEMA = [
    {"key": "document_title", "type": "string", "description": "図書名・タイトル"},
    {"key": "project_name", "type": "string", "description": "工事名"},
    {"key": "project_code", "type": "string", "description": "工事番号"},
    {"key": "location", "type": "string", "description": "工事場所"},
    {"key": "executing_office", "type": "string", "description": "執行課所"},
    {"key": "contract_days", "type": "number", "description": "工期（日数）"},
    {"key": "budget_category", "type": "string", "description": "予算科目"},
    {"key": "quantities", "type": "array", "description": "数量（項目・数量・単位の配列）"},
    {"key": "special_specs", "type": "string", "description": "特記仕様書の要点"},
]

DEFAULT_PARSER = {
    "model": "claude-sonnet-4-20250514",
    "max_tokens": 4096,
}

# データソースごとのDB選択デフォルト
DEFAULT_DB_SELECTION = {
    "sqlite": True,
    "chroma": False,
    "neo4j": False,
    "sqlite_mode": "both",  # "fixed" | "json" | "both"
}


def _load_raw() -> dict:
    """生のJSONを読み込む。"""
    if DATA_SOURCES_PATH.exists():
        try:
            with open(DATA_SOURCES_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            pass
    return {"data_sources": [], "parser": DEFAULT_PARSER.copy()}


def _save_raw(data: dict) -> None:
    """生のJSONを保存する。"""
    DATA_SOURCES_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(DATA_SOURCES_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def get_all_data_sources() -> list[dict]:
    """全データソースを取得する。"""
    data = _load_raw()
    sources = data.get("data_sources", [])
    if not sources:
        # 初回: コリンズ・設計図書をデフォルトで作成
        _ensure_default_sources(data)
        sources = data.get("data_sources", [])
    else:
        _ensure_db_selection_on_sources(data)
        _ensure_design_doc_if_missing(data)
    return sources


def _ensure_default_sources(data: dict) -> None:
    """コリンズ・設計図書がなければデフォルトで作成する。"""
    sources = data.get("data_sources", [])
    existing_names = {s.get("name") for s in sources}

    if "コリンズ" not in existing_names:
        sources.append({
            "id": "corins-default",
            "name": "コリンズ",
            "file_type": "pdf",
            "description": "公共事業の発注情報（工事発注情報）",
            "schema": {"fields": [dict(f) for f in DEFAULT_CORINS_SCHEMA]},
            "parser": data.get("parser", DEFAULT_PARSER),
            "db_selection": dict(DEFAULT_DB_SELECTION),
        })
    if "設計図書" not in existing_names:
        sources.append({
            "id": "design-doc-default",
            "name": "設計図書",
            "file_type": "pdf",
            "description": "設計図書・特記仕様書・設計図面など",
            "schema": {"fields": [dict(f) for f in DEFAULT_DESIGN_DOC_SCHEMA]},
            "parser": data.get("parser", DEFAULT_PARSER),
            "db_selection": dict(DEFAULT_DB_SELECTION),
        })

    data["data_sources"] = sources
    if "parser" not in data:
        data["parser"] = DEFAULT_PARSER.copy()
    _save_raw(data)


def _ensure_db_selection_on_sources(data: dict) -> None:
    """既存データソースにdb_selectionがなければ追加（マイグレーション）。"""
    updated = False
    for ds in data.get("data_sources", []):
        if "db_selection" not in ds:
            ds["db_selection"] = dict(DEFAULT_DB_SELECTION)
            updated = True
    if updated:
        _save_raw(data)


def _ensure_design_doc_if_missing(data: dict) -> None:
    """設計図書がなければ追加する（既存ユーザー向けマイグレーション）。"""
    sources = data.get("data_sources", [])
    if any(s.get("name") == "設計図書" or s.get("id") == "design-doc-default" for s in sources):
        return
    sources.append({
        "id": "design-doc-default",
        "name": "設計図書",
        "file_type": "pdf",
        "description": "設計図書・特記仕様書・設計図面など",
        "schema": {"fields": [dict(f) for f in DEFAULT_DESIGN_DOC_SCHEMA]},
        "parser": data.get("parser", DEFAULT_PARSER),
        "db_selection": dict(DEFAULT_DB_SELECTION),
    })
    data["data_sources"] = sources
    _save_raw(data)


def get_data_source_by_id(source_id: str) -> dict | None:
    """IDでデータソースを1件取得する。"""
    for ds in get_all_data_sources():
        if ds.get("id") == source_id:
            return ds
    return None


def get_data_source_by_name(name: str) -> dict | None:
    """名前でデータソースを1件取得する。"""
    for ds in get_all_data_sources():
        if ds.get("name") == name:
            return ds
    return None


def save_data_source(
    source_id: str | None,
    name: str,
    file_type: str,
    description: str,
    schema: list,
    parser: dict | None = None,
    db_selection: dict | None = None,
) -> str:
    """
    データソースを保存する。新規の場合はidを生成。

    Returns:
        保存したデータソースのID
    """
    data = _load_raw()
    sources = data.get("data_sources", [])
    parser_config = parser or data.get("parser", DEFAULT_PARSER)

    if source_id:
        for i, ds in enumerate(sources):
            if ds.get("id") == source_id:
                db_sel = {**DEFAULT_DB_SELECTION, **ds.get("db_selection", {}), **(db_selection or {})}
                sources[i] = {
                    "id": source_id,
                    "name": name,
                    "file_type": file_type,
                    "description": description,
                    "schema": {"fields": schema},
                    "parser": parser_config,
                    "db_selection": db_sel,
                }
                data["data_sources"] = sources
                _save_raw(data)
                return source_id
        # 見つからなければ新規扱い
    new_id = str(uuid.uuid4())[:8]
    sources.append({
        "id": new_id,
        "name": name,
        "file_type": file_type,
        "description": description,
        "schema": {"fields": schema},
        "parser": parser_config,
        "db_selection": dict(DEFAULT_DB_SELECTION),
    })
    data["data_sources"] = sources
    _save_raw(data)
    return new_id


def delete_data_source(source_id: str) -> bool:
    """データソースを削除する。"""
    data = _load_raw()
    sources = [ds for ds in data.get("data_sources", []) if ds.get("id") != source_id]
    if len(sources) == len(data.get("data_sources", [])):
        return False
    data["data_sources"] = sources
    _save_raw(data)
    return True


def get_parser_config() -> dict:
    """グローバルパーサー設定を取得する。"""
    data = _load_raw()
    return {**DEFAULT_PARSER, **data.get("parser", {})}


def save_parser_config(parser: dict) -> None:
    """グローバルパーサー設定を保存する。"""
    data = _load_raw()
    data["parser"] = parser
    _save_raw(data)


def get_neo4j_config() -> dict:
    """Neo4j接続設定を取得する。data_sources.jsonのneo4j_configから読み込む。"""
    data = _load_raw()
    cfg = data.get("neo4j_config", {})
    return {
        "uri": cfg.get("uri", "bolt://localhost:7687"),
        "user": cfg.get("user", "neo4j"),
        "password": cfg.get("password", ""),
    }


def save_neo4j_config(uri: str, user: str, password: str) -> None:
    """Neo4j接続設定を保存する。"""
    data = _load_raw()
    data["neo4j_config"] = {"uri": uri, "user": user, "password": password}
    _save_raw(data)


def get_db_selection(source_name: str = "コリンズ") -> dict:
    """
    指定データソースのDB選択を取得する。
    工事登録はコリンズを使用するため、デフォルトはコリンズ。
    """
    ds = get_data_source_by_name(source_name) or get_data_source_by_id("corins-default")
    if ds and ds.get("db_selection"):
        return {**DEFAULT_DB_SELECTION, **ds["db_selection"]}
    return dict(DEFAULT_DB_SELECTION)


# ========== 後方互換（corins専用の旧API） ==========

def get_schema(source_name: str = "コリンズ") -> list:
    """
    指定データソースのスキーマを取得する。
    工事登録（コリンズ）用・設計図書用。存在しなければデフォルトを返す。
    """
    id_fallback = "corins-default" if source_name == "コリンズ" else "design-doc-default"
    ds = get_data_source_by_name(source_name) or get_data_source_by_id(id_fallback)
    if ds and ds.get("schema", {}).get("fields"):
        return ds["schema"]["fields"]
    if source_name == "コリンズ":
        return DEFAULT_CORINS_SCHEMA.copy()
    if source_name == "設計図書":
        return DEFAULT_DESIGN_DOC_SCHEMA.copy()
    return []
