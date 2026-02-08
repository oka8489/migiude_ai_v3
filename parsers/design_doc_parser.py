"""
設計図書PDFパーサー
PDFからテキストを抽出し、Claude APIで情報を抽出してJSONで返す。
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
    from services.config_service import get_parser_config
except ImportError:
    get_parser_config = None

DESIGN_DOC_SOURCE_NAME = "設計図書"

# 設計図書はスキーマ設定不要。固定の最小項目のみ抽出
DEFAULT_DESIGN_SCHEMA = [
    {"key": "document_title", "type": "string", "description": "文書タイトル"},
    {"key": "project_name", "type": "string", "description": "工事名"},
    {"key": "project_code", "type": "string", "description": "工事コード"},
]


def extract_text_from_pdf(pdf_path: str) -> str:
    """PDFからテキストを抽出する。"""
    parts = extract_text_from_pdf_by_page(pdf_path)
    return "\n\n".join(t for _, t in parts) if parts else ""


def extract_text_from_pdf_by_page(pdf_path: str) -> list[tuple[int, str]]:
    """
    PDFからページごとにテキストを抽出する。

    Returns:
        [(page_number, text), ...]  page_numberは1始まり
    """
    path = Path(pdf_path)
    if not path.exists():
        raise FileNotFoundError(f"PDFが見つかりません: {pdf_path}")

    result = []
    with pdfplumber.open(pdf_path) as pdf:
        for i, page in enumerate(pdf.pages):
            page_num = i + 1
            t = page.extract_text()
            result.append((page_num, t or ""))
    return result


def _build_schema_prompt(schema: list) -> str:
    """スキーマからプロンプト文字列を生成する。"""
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


def parse_design_doc_pdf(pdf_path: str, pre_extracted_text: str | None = None) -> dict:
    """設計図書PDFを解析し、情報をJSON形式で返す。"""
    text = pre_extracted_text if pre_extracted_text is not None else extract_text_from_pdf(pdf_path)
    if not text or not text.strip():
        raise ValueError(f"PDFからテキストを抽出できませんでした: {pdf_path}")

    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        raise ValueError("ANTHROPIC_API_KEY が設定されていません。")

    schema = DEFAULT_DESIGN_SCHEMA
    parser_config = None
    if get_parser_config:
        parser_config = get_parser_config()
    if parser_config is None:
        parser_config = {"model": "claude-sonnet-4-20250514", "max_tokens": 4096}

    schema_text = _build_schema_prompt(schema)

    system_prompt = f"""あなたは建設工事の設計図書を解析するアシスタントです。
与えられたテキストから、以下の項目を抽出し、必ず有効なJSONのみを1つ返してください。
余計な説明やマークダウンは付けず、JSONオブジェクトだけを出力してください。

抽出する項目（キーは英語のまま）:
{schema_text}

該当する情報が無い項目はnullにしてください。"""

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
    if raw.startswith("```"):
        raw = re.sub(r"^```(?:json)?\s*", "", raw)
        raw = re.sub(r"\s*```$", "", raw)
    return json.loads(raw)
