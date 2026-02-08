"""
コリンズPDFパーサー
PDFからテキストを抽出し、Claude APIで情報を抽出してJSONで返す。
スキーマ・パーサー設定は config_service から読み込む。
"""

import json
import os
import re
from pathlib import Path

import pdfplumber
from dotenv import load_dotenv

load_dotenv()
from anthropic import Anthropic

try:
    from services.config_service import get_schema, get_parser_config
except ImportError:
    get_schema = None
    get_parser_config = None

# 工事登録で使用するデータソース名
CORINS_SOURCE_NAME = "コリンズ"


def extract_text_from_file(file_path: str) -> str:
    """ファイルからテキストを抽出する。PDF・MDに対応。"""
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"ファイルが見つかりません: {file_path}")

    ext = path.suffix.lower()

    if ext == ".pdf":
        text_parts = []
        with pdfplumber.open(file_path) as pdf:
            for page in pdf.pages:
                t = page.extract_text()
                if t:
                    text_parts.append(t)
        return "\n\n".join(text_parts) if text_parts else ""

    if ext == ".md":
        with open(file_path, "r", encoding="utf-8") as f:
            return f.read()

    raise ValueError(f"未対応のファイル形式: {ext}（PDF・MDに対応）")


def _build_schema_prompt(schema: list) -> str:
    """スキーマからClaude用のプロンプト文字列を生成する。"""
    lines = []
    for item in schema:
        key = item.get("key", "")
        desc = item.get("description", "")
        type_hint = item.get("type", "string")
        if type_hint == "array":
            lines.append(f"- {key}: {desc}（配列）")
        elif type_hint == "number":
            lines.append(f"- {key}: {desc}（数値。不明ならnull）")
        elif type_hint == "date":
            lines.append(f"- {key}: {desc}（YYYY-MM-DD形式。不明ならnull）")
        else:
            lines.append(f"- {key}: {desc}（文字列）")
    return "\n".join(lines) if lines else "- （スキーマが空です）"


def extract_with_claude(text: str, schema: list | None = None, parser_config: dict | None = None) -> dict:
    """Claude APIでテキストから指定項目を抽出する。"""
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        raise ValueError(
            "ANTHROPIC_API_KEY が設定されていません。環境変数を設定してください。"
        )

    if schema is None and get_schema:
        schema = get_schema(CORINS_SOURCE_NAME)
    if schema is None:
        schema = []
    if parser_config is None and get_parser_config:
        parser_config = get_parser_config()
    if parser_config is None:
        parser_config = {"model": "claude-sonnet-4-20250514", "max_tokens": 4096}

    schema_text = _build_schema_prompt(schema)
    system_prompt = f"""あなたは公共事業の工事発注情報を解析するアシスタントです。
与えられたテキストから、以下の項目を抽出し、必ず有効なJSONのみを1つ返してください。
余計な説明やマークダウンは付けず、JSONオブジェクトだけを出力してください。

抽出する項目（キーは英語のまま）:
{schema_text}

日付は元の表記からYYYY-MM-DDに変換してください。該当する情報が無い項目はnullにしてください。"""

    user_prompt = f"""以下のテキストから、上記の項目を抽出してJSONで返してください。

--- テキスト ---
{text}
--- ここまで ---"""

    client = Anthropic()
    response = client.messages.create(
        model=parser_config.get("model", "claude-sonnet-4-20250514"),
        max_tokens=parser_config.get("max_tokens", 4096),
        system=system_prompt,
        messages=[{"role": "user", "content": user_prompt}],
    )

    content = response.content[0]
    if content.type != "text":
        raise ValueError("Claude APIがテキスト以外を返しました")

    raw = content.text.strip()
    # コードブロックで囲まれている場合は除去
    if raw.startswith("```"):
        raw = re.sub(r"^```(?:json)?\s*", "", raw)
        raw = re.sub(r"\s*```$", "", raw)
    return json.loads(raw)


def parse_corins_file(file_path: str, pre_extracted_text: str | None = None) -> dict:
    """
    コリンズファイル（PDF/MD）を解析し、登録番号・件名・請負金額などの情報をJSON形式で返す。

    Args:
        file_path: PDFまたはMDファイルのパス
        pre_extracted_text: 事前に抽出したテキスト（Vision API等で取得した場合）

    Returns:
        抽出した情報の辞書。キー: corins_id, project_name, contract_amount,
        start_date, end_date, location, client_name, contractor_name,
        field, work_types, engineers, summary

    Raises:
        FileNotFoundError: ファイルが存在しない場合
        ValueError: ANTHROPIC_API_KEYが未設定、またはClaudeの応答が不正な場合
    """
    text = pre_extracted_text if pre_extracted_text is not None else extract_text_from_file(file_path)
    if not text or not text.strip():
        raise ValueError(f"ファイルからテキストを抽出できませんでした: {file_path}")

    return extract_with_claude(text)
