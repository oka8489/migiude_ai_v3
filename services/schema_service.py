"""
スキーマ自動生成サービス
サンプルドキュメントをClaude APIに送り、抽出すべきフィールド（スキーマ）を自動生成する。
"""

import json
import os
import re
from pathlib import Path

from anthropic import Anthropic

PROJECT_ROOT = Path(__file__).resolve().parent.parent


def extract_text_from_file(file_path: str, file_type: str) -> str:
    """
    ファイルからテキストを抽出する。

    Args:
        file_path: ファイルパス
        file_type: 'pdf' または 'xlsx'

    Returns:
        抽出したテキスト
    """
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"ファイルが見つかりません: {file_path}")

    if file_type == "pdf":
        import pdfplumber
        text_parts = []
        with pdfplumber.open(file_path) as pdf:
            for page in pdf.pages:
                t = page.extract_text()
                if t:
                    text_parts.append(t)
        return "\n\n".join(text_parts) if text_parts else ""

    if file_type == "md":
        with open(file_path, "r", encoding="utf-8") as f:
            return f.read()

    if file_type == "xlsx":
        import openpyxl
        wb = openpyxl.load_workbook(file_path, read_only=True, data_only=True)
        ws = wb.active
        lines = [f"シート名: {ws.title}", ""]
        for row in ws.iter_rows():
            for cell in row:
                if cell.value is not None:
                    lines.append(f"{cell.coordinate}: {cell.value}")
        return "\n".join(lines)

    raise ValueError(f"未対応のファイル型: {file_type}")


def extract_schema_from_sample(
    sample_text: str,
    source_name: str,
    description: str = "",
) -> dict:
    """
    サンプルテキストをClaude APIに送り、抽出すべきスキーマを自動生成する。

    Args:
        sample_text: サンプルファイルのテキスト
        source_name: データソース名（コリンズ、設計書、見積書など）
        description: データソースの説明（任意）

    Returns:
        {"fields": [{"key": "xxx", "type": "string|number|date|array", "description": "..."}, ...]}
    """
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        raise ValueError("ANTHROPIC_API_KEY が設定されていません。環境変数を設定してください。")

    desc_part = f"\n説明: {description}" if description else ""
    text_preview = sample_text[:8000]  # トークン制限のため

    system_prompt = """あなたはドキュメント解析のアシスタントです。
与えられたサンプルドキュメントの内容を分析し、この種のドキュメントから抽出すべきフィールド（項目）を特定してください。
各フィールドについて、キー（英語・スネークケース推奨）、型、説明を返してください。

必ず有効なJSONのみを1つ返してください。余計な説明やマークダウンは付けず、JSONオブジェクトだけを出力してください。

返却形式:
{
  "fields": [
    {"key": "field_name", "type": "string|number|date|array|object", "description": "説明"},
    ...
  ]
}

型の目安:
- string: テキスト
- number: 数値（金額、数量など）
- date: 日付（YYYY-MM-DD形式で抽出）
- array: 複数値（工種のリスト、技術者リストなど）
- object: ネストしたオブジェクト"""

    user_prompt = f"""以下は「{source_name}」というデータソースのサンプルです。{desc_part}

【サンプルの内容】
{text_preview}

このドキュメントから抽出すべきフィールドを分析し、上記のJSON形式で返してください。"""

    client = Anthropic()
    response = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=4096,
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

    data = json.loads(raw)
    fields = data.get("fields", [])
    # "name" を "key" に正規化、keyを英語スネークケースに
    normalized = []
    for f in fields:
        key = f.get("key") or f.get("name", "field")
        if isinstance(key, str):
            key = key.lower().replace(" ", "_").replace("　", "_").replace("-", "_")
        normalized.append({
            "key": key,
            "type": f.get("type", "string"),
            "description": f.get("description", ""),
        })
    return {"fields": normalized}
