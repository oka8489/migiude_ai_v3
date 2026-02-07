"""
Neo4j サービス - 建設業ナレッジグラフ

グラフ構造:
  Project を中心に、発注者・受注者・技術者・工種・地域・材料・工法 等を
  ノードとリレーションシップで接続。工事間の横断検索を可能にする。
"""

import json
import os
import re

from dotenv import load_dotenv

load_dotenv()


def _get_config():
    """Neo4j接続設定を取得。.env を優先し、未設定時は config にフォールバック。"""
    try:
        from services.config_service import get_neo4j_config

        cfg = get_neo4j_config()
        return (
            os.environ.get("NEO4J_URI") or cfg.get("uri") or "bolt://localhost:7687",
            os.environ.get("NEO4J_USER") or cfg.get("user") or "neo4j",
            os.environ.get("NEO4J_PASSWORD") or cfg.get("password") or "",
        )
    except ImportError:
        return (
            os.environ.get("NEO4J_URI", "bolt://localhost:7687"),
            os.environ.get("NEO4J_USER", "neo4j"),
            os.environ.get("NEO4J_PASSWORD", ""),
        )


def _try_connect(uri: str, user: str, password: str):
    """接続を試行。成功時はdriver、失敗時は(False, error_msg)。"""
    try:
        from neo4j import GraphDatabase

        driver = GraphDatabase.driver(uri, auth=(user, password))
        driver.verify_connectivity()
        return driver
    except Exception as e:
        return (False, str(e))


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


def get_driver():
    """Neo4jドライバーを取得。接続不可時はNone。"""
    return _get_working_driver()


def is_neo4j_available() -> bool:
    """Neo4jが利用可能か（パッケージ・接続）。"""
    uri, user, password = _get_config()
    if not password:
        return False
    result = _try_connect(uri, user, password)
    if isinstance(result, tuple):
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
    if "neo4j+s://" in (uri or ""):
        alt_uri = uri.replace("neo4j+s://", "neo4j+ssc://")
        result2 = _try_connect(alt_uri, user, password)
        if not isinstance(result2, tuple):
            result2.close()
            return None
    return err


def _split_csv(value) -> list:
    """カンマ・読点・改行区切りの文字列、またはリストをリストに正規化。"""
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    s = str(value).strip()
    if not s:
        return []
    items = re.split(r"[,、\n]+", s)
    return [item.strip() for item in items if item.strip()]


def _extract_fiscal_year(start_date: str) -> str:
    """開始日から年度を推定（4月始まり）"""
    if not start_date:
        return ""
    start_date = str(start_date)
    match = re.search(r"(\d{4})[/-](\d{1,2})", start_date)
    if match:
        year, month = int(match.group(1)), int(match.group(2))
        if month < 4:
            year -= 1
        return f"{year}年度"
    match = re.search(r"令和(\d+)年", start_date)
    if match:
        year = 2018 + int(match.group(1))
        return f"{year}年度"
    match = re.search(r"平成(\d+)年", start_date)
    if match:
        year = 1988 + int(match.group(1))
        return f"{year}年度"
    return ""


def _extract_prefecture(location: str) -> str:
    """住所から都道府県を抽出"""
    if not location:
        return ""
    location = str(location)
    match = re.search(r"(北海道|.{2,3}[都府県])", location)
    return match.group(1) if match else ""


def _extract_region_parts(location: str) -> list[str]:
    """
    住所から地域を県・市（区町村）で別々に抽出する。
    「大分県日田市」→ ["大分県", "日田市"] のように1ノードにせず別々に返す。
    """
    if not location:
        return []
    loc = str(location).strip()
    parts = []

    # 都道府県
    pref_match = re.search(r"(北海道|.{2,3}[都府県])", loc)
    if pref_match:
        parts.append(pref_match.group(1))

    # 市区町村郡（県の後の部分）
    city_match = re.search(r"[都府県]([^、,，\s]*(?:市|区|町|村|郡))", loc)
    if city_match:
        city = city_match.group(1).strip()
        if city and (not pref_match or city != pref_match.group(1)):
            parts.append(city)

    return parts


def _normalize_project_data(project: dict) -> dict:
    """project（SQLite形式）をparsed_data形式に正規化。"""
    raw = project.get("raw_json")
    if raw:
        try:
            data = json.loads(raw) if isinstance(raw, str) else raw
            data["project_name"] = data.get("project_name") or project.get("project_name", "")
            data["project_code"] = project.get("project_code", "")
            data["client_name"] = data.get("client_name") or project.get("client_name", "")
            data["contractor_name"] = data.get("contractor_name") or project.get("contractor_name", "")
            data["location"] = data.get("location") or project.get("location", "")
            data["field"] = data.get("field") or project.get("field", "")
            data["start_date"] = data.get("start_date") or project.get("start_date", "")
            data["end_date"] = data.get("end_date") or project.get("end_date", "")
            return data
        except (json.JSONDecodeError, TypeError):
            pass
    return project


def save_project_to_neo4j(project: dict) -> bool:
    """
    工事データをNeo4jに保存。コリンズデータからグラフを構築。

    リレーション（Project始点で統一）:
      ORDERED_BY→Client, CONTRACTED_BY→Contractor, CONTRACTED_BY→ContractMethod,
      HAS_ENGINEER→Engineer, IN_FIELD→Field, IN_FISCAL_YEAR→FiscalYear,
      HAS_WORK_TYPE→WorkType, USES_METHOD→ConstructionMethod,
      LOCATED_IN→Region（県・市を別ノード）, ON_ROUTE→Route,
      IN_AREA_TYPE→ConstructionArea, IN_BID_CATEGORY→BidCategory,
      REQUIRES_PERMIT→PermitType
    """
    driver = _get_working_driver()
    if not driver:
        return False

    data = _normalize_project_data(project)
    project_id = project.get("id")
    if not project_id:
        return False

    raw_json = json.dumps(data, ensure_ascii=False)

    try:
        with driver.session() as session:
            # ---------- Project ノード ----------
            session.run(
                """
                MERGE (p:Project {sqlite_id: $sqlite_id})
                SET p.name = $name,
                    p.project_code = $project_code,
                    p.corins_id = $corins_id,
                    p.contract_amount = $contract_amount,
                    p.start_date = $start_date,
                    p.end_date = $end_date,
                    p.location = $location,
                    p.summary = $summary,
                    p.raw_json = $raw_json,
                    p.updated_at = datetime()
                """,
                sqlite_id=project_id,
                name=data.get("project_name", ""),
                project_code=data.get("project_code", "") or project.get("project_code", ""),
                corins_id=data.get("corins_id", ""),
                contract_amount=data.get("contract_amount", ""),
                start_date=data.get("start_date", ""),
                end_date=data.get("end_date", ""),
                location=data.get("location", ""),
                summary=data.get("summary", ""),
                raw_json=raw_json,
            )

            # ---------- Client（発注者）ORDERED_BY: Project → Client ----------
            client = data.get("client_name", "")
            if client:
                session.run(
                    """
                    MERGE (c:Client {name: $name})
                    WITH c
                    MATCH (p:Project {sqlite_id: $sid})
                    MERGE (p)-[:ORDERED_BY]->(c)
                    """,
                    name=client,
                    sid=project_id,
                )
                pref = _extract_prefecture(data.get("location", ""))
                if pref:
                    session.run(
                        """
                        MERGE (c:Client {name: $client})
                        MERGE (r:Region {name: $pref})
                        MERGE (c)-[:MANAGES]->(r)
                        """,
                        client=client,
                        pref=pref,
                    )

            # ---------- Contractor（受注者）CONTRACTED_BY: Project → Contractor ----------
            contractor = data.get("contractor_name", "")
            if contractor:
                session.run(
                    """
                    MERGE (co:Contractor {name: $name})
                    WITH co
                    MATCH (p:Project {sqlite_id: $sid})
                    MERGE (p)-[:CONTRACTED_BY]->(co)
                    """,
                    name=contractor,
                    sid=project_id,
                )

            # ---------- Engineer（技術者）HAS_ENGINEER: Project → Engineer ----------
            engineers = data.get("engineers", [])
            if isinstance(engineers, str):
                engineers = _split_csv(engineers)

            for eng in engineers:
                if isinstance(eng, dict):
                    name = eng.get("name", "")
                    role = eng.get("role", "技術者")
                elif isinstance(eng, str):
                    role = "技術者"
                    name = eng
                    if ":" in eng or "：" in eng:
                        parts = re.split(r"[:：]", eng, maxsplit=1)
                        role = parts[0].strip()
                        name = parts[1].strip()
                else:
                    continue

                if not name:
                    continue

                session.run(
                    """
                    MERGE (e:Engineer {name: $name})
                    WITH e
                    MATCH (p:Project {sqlite_id: $sid})
                    MERGE (p)-[:HAS_ENGINEER {role: $role}]->(e)
                    """,
                    name=name,
                    sid=project_id,
                    role=role,
                )

            # ---------- WorkType（工種）----------
            work_types_raw = data.get("work_types", "")
            work_types = _split_csv(work_types_raw)
            if not work_types and isinstance(data.get("work_types"), list):
                work_types = [str(w) for w in data.get("work_types", []) if w]
            for wt in work_types:
                if wt:
                    session.run(
                        """
                        MERGE (w:WorkType {name: $name})
                        WITH w
                        MATCH (p:Project {sqlite_id: $sid})
                        MERGE (p)-[:HAS_WORK_TYPE]->(w)
                        """,
                        name=wt,
                        sid=project_id,
                    )

            # ---------- Field（分野）----------
            field = data.get("field", "")
            if field:
                session.run(
                    """
                    MERGE (f:Field {name: $name})
                    WITH f
                    MATCH (p:Project {sqlite_id: $sid})
                    MERGE (p)-[:IN_FIELD]->(f)
                    """,
                    name=field,
                    sid=project_id,
                )

            # ---------- Region（地域）県と市を別ノードで作成 LOCATED_IN: Project → Region ----------
            location = data.get("location", "")
            region_parts = _extract_region_parts(location)
            for region_name in region_parts:
                if region_name:
                    session.run(
                        """
                        MERGE (r:Region {name: $name})
                        WITH r
                        MATCH (p:Project {sqlite_id: $sid})
                        MERGE (p)-[:LOCATED_IN]->(r)
                        """,
                        name=region_name,
                        sid=project_id,
                    )
            # 市が県に含まれる関係を設定（県と市が両方ある場合）
            if len(region_parts) >= 2:
                session.run(
                    """
                    MERGE (r_pref:Region {name: $pref})
                    MERGE (r_city:Region {name: $city})
                    MERGE (r_city)-[:PART_OF]->(r_pref)
                    """,
                    pref=region_parts[0],
                    city=region_parts[1],
                )

            # ---------- FiscalYear（年度）----------
            fy = _extract_fiscal_year(data.get("start_date", ""))
            if fy:
                session.run(
                    """
                    MERGE (fy:FiscalYear {name: $name})
                    WITH fy
                    MATCH (p:Project {sqlite_id: $sid})
                    MERGE (p)-[:IN_FISCAL_YEAR]->(fy)
                    """,
                    name=fy,
                    sid=project_id,
                )

            # ---------- Route（路線）----------
            route = data.get("target_route_name", "")
            if route:
                session.run(
                    """
                    MERGE (rt:Route {name: $name})
                    WITH rt
                    MATCH (p:Project {sqlite_id: $sid})
                    MERGE (p)-[:ON_ROUTE]->(rt)
                    """,
                    name=route,
                    sid=project_id,
                )

            # ---------- ContractMethod（契約方式）----------
            method = data.get("contract_method", "")
            if method:
                session.run(
                    """
                    MERGE (cm:ContractMethod {name: $name})
                    WITH cm
                    MATCH (p:Project {sqlite_id: $sid})
                    MERGE (p)-[:CONTRACTED_BY]->(cm)
                    """,
                    name=method,
                    sid=project_id,
                )

            # ---------- ConstructionPermitType（許可業種）----------
            permit_type = data.get("construction_permit_type", "")
            if permit_type:
                session.run(
                    """
                    MERGE (cp:PermitType {name: $name})
                    WITH cp
                    MATCH (p:Project {sqlite_id: $sid})
                    MERGE (p)-[:REQUIRES_PERMIT]->(cp)
                    """,
                    name=permit_type,
                    sid=project_id,
                )

            # ---------- BidCategory（入札区分）----------
            bid_cat = data.get("bid_qualification_category", "")
            if bid_cat:
                session.run(
                    """
                    MERGE (bc:BidCategory {name: $name})
                    WITH bc
                    MATCH (p:Project {sqlite_id: $sid})
                    MERGE (p)-[:IN_BID_CATEGORY]->(bc)
                    """,
                    name=bid_cat,
                    sid=project_id,
                )

            # ---------- ConstructionArea（施工地域区分）----------
            area = data.get("construction_area", "")
            if area:
                session.run(
                    """
                    MERGE (ca:ConstructionArea {name: $name})
                    WITH ca
                    MATCH (p:Project {sqlite_id: $sid})
                    MERGE (p)-[:IN_AREA_TYPE]->(ca)
                    """,
                    name=area,
                    sid=project_id,
                )

            # ---------- ConstructionMethod（工法）----------
            methods = data.get("construction_methods", [])
            if isinstance(methods, str):
                methods = _split_csv(methods)
            elif not isinstance(methods, list):
                methods = []
            for m in methods:
                m_name = m if isinstance(m, str) else str(m)
                if m_name.strip():
                    session.run(
                        """
                        MERGE (cm:ConstructionMethod {name: $name})
                        WITH cm
                        MATCH (p:Project {sqlite_id: $sid})
                        MERGE (p)-[:USES_METHOD]->(cm)
                        """,
                        name=m_name.strip(),
                        sid=project_id,
                    )

            # ---------- Project プロパティに追加情報をSET ----------
            session.run(
                """
                MATCH (p:Project {sqlite_id: $sid})
                SET p.night_work = $night_work,
                    p.traffic_regulation = $traffic_regulation,
                    p.road_traffic_volume = $road_traffic_volume,
                    p.traffic_control_method = $traffic_control_method,
                    p.construction_area = $construction_area,
                    p.order_type = $order_type,
                    p.coordinates = $coordinates,
                    p.construction_permit_number = $permit_number
                """,
                sid=project_id,
                night_work=data.get("night_work", ""),
                traffic_regulation=data.get("traffic_regulation", ""),
                road_traffic_volume=data.get("road_traffic_volume", ""),
                traffic_control_method=data.get("traffic_control_method", ""),
                construction_area=data.get("construction_area", ""),
                order_type=data.get("order_type", ""),
                coordinates=(
                    data.get("coordinates", "")
                    or data.get("start_location_coordinates", "")
                    or data.get("end_location_coordinates", "")
                ),
                permit_number=data.get("construction_permit_number", ""),
            )

            # ---------- Contractor にも詳細プロパティ追加 ----------
            contractor = data.get("contractor_name", "")
            if contractor:
                session.run(
                    """
                    MATCH (co:Contractor {name: $name})
                    SET co.contractor_id = $cid,
                        co.address = $address,
                        co.tel = $tel,
                        co.fax = $fax,
                        co.permit_number = $permit
                    """,
                    name=contractor,
                    cid=data.get("contractor_id", ""),
                    address=data.get("office_address", ""),
                    tel=data.get("office_tel", ""),
                    fax=data.get("office_fax", ""),
                    permit=data.get("construction_permit_number", ""),
                )

            # ---------- Client にも詳細プロパティ追加 ----------
            client = data.get("client_name", "")
            if client:
                session.run(
                    """
                    MATCH (c:Client {name: $name})
                    SET c.address = $address,
                        c.tel = $tel,
                        c.postal_code = $postal
                    """,
                    name=client,
                    address=data.get("ordering_agency_address", ""),
                    tel=data.get("ordering_agency_tel", ""),
                    postal=data.get("ordering_agency_postal_code", ""),
                )

        return True
    except Exception:
        return False
    finally:
        driver.close()


def save_design_doc_to_neo4j(parsed_data: dict, project_id: int) -> bool:
    """
    設計図書データをNeo4jに保存。

    作成するノード・リレーション:
      DesignDocument, BudgetCategory, Material, Method, Regulation
    """
    driver = _get_working_driver()
    if not driver:
        return False

    try:
        with driver.session() as session:
            # Project が存在するか確認
            result = session.run(
                "MATCH (p:Project {sqlite_id: $pid}) RETURN p",
                pid=project_id,
            )
            if not result.single():
                return False

            # ---------- DesignDocument ノード ----------
            session.run(
                """
                MATCH (p:Project {sqlite_id: $pid})
                CREATE (d:DesignDocument {
                    document_title: $document_title,
                    project_name: $project_name,
                    project_code: $project_code,
                    location: $location,
                    executing_office: $executing_office,
                    contract_days: $contract_days,
                    quantities: $quantities,
                    special_specs: $special_specs,
                    raw_json: $raw_json,
                    created_at: datetime()
                })
                MERGE (p)-[:HAS_DESIGN_DOC]->(d)
                """,
                pid=project_id,
                document_title=parsed_data.get("document_title", ""),
                project_name=parsed_data.get("project_name", ""),
                project_code=parsed_data.get("project_code", ""),
                location=parsed_data.get("location", ""),
                executing_office=parsed_data.get("executing_office", ""),
                contract_days=parsed_data.get("contract_days", ""),
                quantities=(
                    json.dumps(parsed_data.get("quantities"), ensure_ascii=False)
                    if isinstance(parsed_data.get("quantities"), (list, dict))
                    else str(parsed_data.get("quantities", ""))
                ),
                special_specs=parsed_data.get("special_specs", ""),
                raw_json=json.dumps(parsed_data, ensure_ascii=False),
            )

            # ---------- BudgetCategory（予算区分）----------
            budget = parsed_data.get("budget_category", "")
            if budget:
                session.run(
                    """
                    MERGE (b:BudgetCategory {name: $name})
                    WITH b
                    MATCH (p:Project {sqlite_id: $pid})
                    MERGE (p)-[:HAS_BUDGET_CATEGORY]->(b)
                    """,
                    name=budget,
                    pid=project_id,
                )

            # ---------- Material（使用材料）----------
            quantities = parsed_data.get("quantities", "")
            for mat in _split_csv(quantities):
                mat_name = re.sub(
                    r"[\d,.]+\s*[a-zA-Zｍ㎡㎥ｔ]*$", "", str(mat)
                ).strip()
                if mat_name:
                    session.run(
                        """
                        MERGE (m:Material {name: $name})
                        WITH m
                        MATCH (p:Project {sqlite_id: $pid})
                        MERGE (p)-[:USES_MATERIAL]->(m)
                        """,
                        name=mat_name,
                        pid=project_id,
                    )

            # ---------- special_specs から工法・法規制を抽出 ----------
            specs = parsed_data.get("special_specs", "")
            if specs:
                specs_str = (
                    specs if isinstance(specs, str) else json.dumps(specs, ensure_ascii=False)
                )
                method_keywords = [
                    "工法",
                    "施工",
                    "打設",
                    "転圧",
                    "注入",
                    "吹付",
                    "切削",
                    "オーバーレイ",
                    "プレキャスト",
                ]
                reg_keywords = [
                    "法",
                    "規則",
                    "基準",
                    "指針",
                    "告示",
                    "条例",
                    "省令",
                    "通達",
                    "要領",
                ]
                for line in _split_csv(specs_str):
                    if any(kw in line for kw in method_keywords):
                        session.run(
                            """
                            MERGE (m:Method {name: $name})
                            WITH m
                            MATCH (p:Project {sqlite_id: $pid})
                            MERGE (p)-[:USES_METHOD]->(m)
                            """,
                            name=line,
                            pid=project_id,
                        )
                    if any(kw in line for kw in reg_keywords):
                        session.run(
                            """
                            MERGE (reg:Regulation {name: $name})
                            WITH reg
                            MATCH (p:Project {sqlite_id: $pid})
                            MERGE (p)-[:REQUIRES_REGULATION]->(reg)
                            """,
                            name=line,
                            pid=project_id,
                        )

        return True
    except Exception:
        return False
    finally:
        driver.close()


def delete_project_from_neo4j(project_id: int) -> bool:
    """SQLite IDに紐づくProjectノードと関連DesignDocumentを削除。"""
    driver = _get_working_driver()
    if not driver:
        return False

    try:
        with driver.session() as session:
            session.run(
                """
                MATCH (p:Project {sqlite_id: $sid})-[:HAS_DESIGN_DOC]->(d:DesignDocument)
                DETACH DELETE d
                """,
                sid=project_id,
            )
            session.run(
                "MATCH (p:Project {sqlite_id: $sid}) DETACH DELETE p",
                sid=project_id,
            )
            session.run(
                """
                MATCH (n)
                WHERE NOT (n)--()
                AND (n:Material OR n:Method OR n:Regulation OR n:WorkType
                     OR n:BudgetCategory OR n:Field OR n:Region OR n:FiscalYear
                     OR n:Client OR n:Contractor OR n:Engineer
                     OR n:Route OR n:ContractMethod OR n:PermitType
                     OR n:BidCategory OR n:ConstructionArea OR n:ConstructionMethod)
                DELETE n
                """
            )
        return True
    except Exception:
        return False
    finally:
        driver.close()
