"""
Neo4jサービス
工事データをグラフDBに保存。関係性（発注者、場所、工種など）をノード・リレーションで表現。
"""

import os

from dotenv import load_dotenv

load_dotenv()


def _get_config():
    """Neo4j接続設定を取得。"""
    try:
        from services.config_service import get_neo4j_config
        cfg = get_neo4j_config()
        return (
            cfg.get("uri") or os.environ.get("NEO4J_URI", "bolt://localhost:7687"),
            cfg.get("user") or os.environ.get("NEO4J_USER", "neo4j"),
            cfg.get("password") or os.environ.get("NEO4J_PASSWORD", ""),
        )
    except ImportError:
        return (
            os.environ.get("NEO4J_URI", "bolt://localhost:7687"),
            os.environ.get("NEO4J_USER", "neo4j"),
            os.environ.get("NEO4J_PASSWORD", ""),
        )


def _get_working_driver():
    """接続可能なNeo4jドライバーを取得。neo4j+s失敗時はneo4j+sscを試す。"""
    uri, user, password = _get_config()
    if not password:
        return None
    result = _try_connect(uri, user, password)
    if not isinstance(result, tuple):
        return result
    if "neo4j+s://" in (uri or ""):
        alt_uri = uri.replace("neo4j+s://", "neo4j+ssc://")
        result2 = _try_connect(alt_uri, user, password)
        if not isinstance(result2, tuple):
            return result2
    return None


def _get_driver():
    """Neo4jドライバーを取得（後方互換）。"""
    return _get_working_driver()


def _try_connect(uri: str, user: str, password: str):
    """接続を試行。成功時はdriver、失敗時は(False, error_msg)。"""
    try:
        from neo4j import GraphDatabase
        driver = GraphDatabase.driver(uri, auth=(user, password))
        driver.verify_connectivity()
        return driver
    except Exception as e:
        return (False, str(e))


def is_neo4j_available() -> bool:
    """Neo4jが利用可能か（パッケージ・接続）。"""
    uri, user, password = _get_config()
    if not password:
        return False
    result = _try_connect(uri, user, password)
    if isinstance(result, tuple):
        # neo4j+s:// で失敗した場合、neo4j+ssc:// を試す（AuraDBのSSL証明書問題対策）
        if "neo4j+s://" in (uri or ""):
            alt_uri = uri.replace("neo4j+s://", "neo4j+ssc://")
            result2 = _try_connect(alt_uri, user, password)
            if not isinstance(result2, tuple):
                result2.close()
                return True
        return False
    result.close()
    return True


def get_neo4j_connection_error() -> str | None:
    """接続失敗時のエラーメッセージを返す。成功時はNone。"""
    uri, user, password = _get_config()
    if not password:
        return "パスワードが設定されていません。設定画面でNeo4j接続情報を入力してください。"
    result = _try_connect(uri, user, password)
    if not isinstance(result, tuple):
        result.close()
        return None
    _, err = result
    # neo4j+s:// で失敗した場合、neo4j+ssc:// を試す
    if "neo4j+s://" in (uri or ""):
        alt_uri = uri.replace("neo4j+s://", "neo4j+ssc://")
        result2 = _try_connect(alt_uri, user, password)
        if not isinstance(result2, tuple):
            result2.close()
            return None
    return err


def save_project_to_neo4j(project: dict) -> bool:
    """
    工事データをNeo4jに保存する。
    ノード: Project, Client, Contractor, Location
    リレーション: Project-[:発注者]->Client, Project-[:受注者]->Contractor, Project-[:場所]->Location

    Args:
        project: 工事データ（id, project_code, project_name, client_name, contractor_name, location等）

    Returns:
        成功したらTrue
    """
    driver = _get_working_driver()
    if not driver:
        return False

    try:
        with driver.session() as session:
            pid = project.get("id")
            code = project.get("project_code", "")
            name = project.get("project_name", "")
            project_type = project.get("project_type", "")
            amount = project.get("contract_amount")
            start_date = project.get("start_date")
            end_date = project.get("end_date")
            client_name = project.get("client_name", "")
            contractor_name = project.get("contractor_name", "")
            location = project.get("location", "")
            field = project.get("field", "")

            # Projectノード作成（MERGEで重複防止、idで一意）
            session.run("""
                MERGE (p:Project {sqlite_id: $sqlite_id})
                SET p.project_code = $project_code,
                    p.project_name = $project_name,
                    p.project_type = $project_type,
                    p.contract_amount = $contract_amount,
                    p.start_date = $start_date,
                    p.end_date = $end_date,
                    p.field = $field
            """, sqlite_id=pid, project_code=code, project_name=name, project_type=project_type,
                contract_amount=amount, start_date=start_date, end_date=end_date, field=field)

            # 発注者ノードとリレーション
            if client_name:
                session.run("""
                    MERGE (c:Client {name: $name})
                    WITH c
                    MATCH (p:Project {sqlite_id: $sqlite_id})
                    MERGE (p)-[:発注者]->(c)
                """, name=client_name, sqlite_id=pid)

            # 受注者ノードとリレーション
            if contractor_name:
                session.run("""
                    MERGE (co:Contractor {name: $name})
                    WITH co
                    MATCH (p:Project {sqlite_id: $sqlite_id})
                    MERGE (p)-[:受注者]->(co)
                """, name=contractor_name, sqlite_id=pid)

            # 場所ノードとリレーション
            if location:
                session.run("""
                    MERGE (l:Location {name: $name})
                    WITH l
                    MATCH (p:Project {sqlite_id: $sqlite_id})
                    MERGE (p)-[:場所]->(l)
                """, name=location, sqlite_id=pid)

            # 工種（WorkType）ノードとリレーション
            work_types = project.get("work_types") or []
            if isinstance(work_types, str):
                try:
                    import json
                    work_types = json.loads(work_types)
                except Exception:
                    work_types = []
            for wt in work_types:
                if wt:
                    session.run("""
                        MERGE (w:WorkType {name: $name})
                        WITH w
                        MATCH (p:Project {sqlite_id: $sqlite_id})
                        MERGE (p)-[:工種]->(w)
                    """, name=str(wt), sqlite_id=pid)

        return True
    except Exception:
        return False
    finally:
        driver.close()


def delete_project_from_neo4j(sqlite_id: int) -> bool:
    """Neo4jから工事ノードと関連リレーションを削除。"""
    driver = _get_driver()
    if not driver:
        return False
    try:
        with driver.session() as session:
            session.run("""
                MATCH (p:Project {sqlite_id: $sqlite_id})
                DETACH DELETE p
            """, sqlite_id=sqlite_id)
        return True
    except Exception:
        return False
    finally:
        driver.close()
