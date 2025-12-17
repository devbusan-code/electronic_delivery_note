# install dependencies:
# uv add python-dotenv pymysql

import json
import os
import urllib.error
import urllib.request
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Optional, Tuple

import pymysql
from dotenv import load_dotenv
from logger_kki import LoggerKKI


# kki_logger.py에서 logger 객체를 가져옵니다.
logger = LoggerKKI().get_logger()

# .env 파일 로드
load_dotenv()

ZERO_DECIMAL = Decimal("0")
_SAHACACODE_COLUMN_CACHE: Optional[Dict[str, str]] = None

SAHACACODE_COLUMN_CANDIDATES: Dict[str, Tuple[str, ...]] = {
    "selfGoodCd": (
        "selfGoodCd",
        "self_good_cd",
        "SELFGOODCD",
        "SELF_GOOD_CD",
        "selfgoodcd",
        "SELFGOODCD",
        "hacaitem",
    ),
    "unitQuantity": (
        "unitQuantity",
        "unit_quantity",
        "UNITQUANTITY",
        "UNIT_QUANTITY",
        "unitqty",
        "unitQty",
        "hacaweit",
    ),
    "hacaamnt": (
        "hacaamnt",
        "haca_amnt",
        "HACAAMNT",
        "HACA_AMNT",
        "hacaAmt",
        "HACAAMT",
        "haca_amount",
    ),
}


def build_api_url(service_key: str, ship_date: str = "20251117", page_no: int = 1) -> str:
    return (
        f"https://at.agromarket.kr/openApi/inven/list.do?serviceKey={service_key}"
        f"&shipDate={ship_date}&pageNo={page_no}"
    )


def _clip(value, max_len: int):
    """문자열 길이가 초과되면 잘라서 반환."""
    if value is None:
        return None
    return str(value)[:max_len]


def _normalize_good_code(value) -> Optional[str]:
    if value is None:
        return None
    value_str = str(value).strip()
    return value_str or None


def _normalize_unit_quantity(value) -> Optional[str]:
    if value is None:
        return None
    try:
        decimal_value = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        fallback = str(value).strip()
        return fallback or None
    normalized = decimal_value.normalize()
    if normalized == normalized.to_integral():
        return str(normalized.to_integral())
    normalized_str = format(normalized, "f").rstrip("0").rstrip(".")
    return normalized_str or "0"


def _to_decimal(value) -> Decimal:
    if value is None:
        return ZERO_DECIMAL
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return ZERO_DECIMAL


def populate_sahaca_amount(conn, rows: List[Dict[str, Any]]):
    if not rows:
        return

    goods = {_normalize_good_code(row.get("selfGoodCd")) for row in rows}
    goods.discard(None)

    if not goods:
        for row in rows:
            row["sahaca_amount"] = ZERO_DECIMAL
        return

    column_map = _resolve_sahacacode_columns(conn)
    self_good_col = column_map["selfGoodCd"]
    unit_qty_col = column_map["unitQuantity"]
    haca_col = column_map["hacaamnt"]

    placeholders = ",".join(["%s"] * len(goods))
    query = (
        f"SELECT {self_good_col}, {unit_qty_col}, {haca_col} "
        f"FROM sahacacode WHERE {self_good_col} IN ({placeholders})"
    )

    with conn.cursor() as cur:
        cur.execute(query, tuple(goods))
        records = cur.fetchall()

    lookup: Dict[Tuple[Optional[str], Optional[str]], Decimal] = {}
    for self_good_cd, unit_quantity, haca_amnt in records:
        key = (
            _normalize_good_code(self_good_cd),
            _normalize_unit_quantity(unit_quantity),
        )
        lookup[key] = _to_decimal(haca_amnt)

    for row in rows:
        good_code = _normalize_good_code(row.get("selfGoodCd"))
        unit_qty = _normalize_unit_quantity(row.get("unitQuantity"))
        haca = lookup.get((good_code, unit_qty), ZERO_DECIMAL)
        ship_qty = _to_decimal(row.get("shipQuantity"))
        row["sahaca_amount"] = haca * ship_qty


def update_daily_unloading_cost_total(conn, inven_nos: List[str]):
    unique_inven_nos = sorted({inven_no for inven_no in inven_nos if inven_no})
    if not unique_inven_nos:
        return

    placeholders = ",".join(["%s"] * len(unique_inven_nos))
    impacted_sql = f"""
    SELECT DISTINCT
        m.shipDate,
        cm.chulcode,
        cm.chcdcode,
        d.selfSanCd,
        d.selfGoodCd
    FROM electronic_delivery_note_detail d
    JOIN electronic_delivery_note_master m ON m.invenNo = d.invenNo
    JOIN chulcode_matching cm ON cm.proDecNo = d.proDecNo
    WHERE d.invenNo IN ({placeholders})
        AND cm.chulcode IS NOT NULL
        AND cm.chcdcode IS NOT NULL
    """

    with conn.cursor() as cur:
        cur.execute(impacted_sql, tuple(unique_inven_nos))
        impacted_rows = [
            row for row in cur.fetchall() if all(value is not None for value in row)
        ]

    if not impacted_rows:
        logger.info(
            "일별 하차비 집계 대상 없음 (invenNo %d건)", len(unique_inven_nos)
        )
        return

    combo_placeholders = ",".join(["(%s,%s,%s,%s,%s)"] * len(impacted_rows))
    combo_params: List[Any] = []
    for ship_date, chulcode, chcdcode, self_san_cd, self_good_cd in impacted_rows:
        combo_params.extend(
            [ship_date, chulcode, chcdcode, self_san_cd, self_good_cd]
        )

    sum_sql = f"""
    SELECT
        m.shipDate,
        cm.chulcode,
        cm.chcdcode,
        d.selfSanCd,
        d.selfGoodCd,
        SUM(COALESCE(d.sahaca_amount, 0)) AS total_cost
    FROM electronic_delivery_note_detail d
    JOIN electronic_delivery_note_master m ON m.invenNo = d.invenNo
    JOIN chulcode_matching cm ON cm.proDecNo = d.proDecNo
    WHERE (m.shipDate, cm.chulcode, cm.chcdcode, d.selfSanCd, d.selfGoodCd)
        IN ({combo_placeholders})
    GROUP BY 1, 2, 3, 4, 5
    """

    with conn.cursor() as cur:
        cur.execute(sum_sql, tuple(combo_params))
        totals = cur.fetchall()

    if not totals:
        logger.info("일별 하차비 합계 계산 결과 없음")
        return

    insert_sql = """
    INSERT INTO daily_unloading_cost_total (
        shipdate, chulcode, chcdcode, selfSanCd, selfGoodCd, unloading_cost_total
    ) VALUES (
        %s, %s, %s, %s, %s, %s
    )
    ON DUPLICATE KEY UPDATE
        unloading_cost_total = VALUES(unloading_cost_total)
    """

    with conn.cursor() as cur:
        cur.executemany(insert_sql, totals)

    logger.info("일별 하차비 합계 %d건 반영", len(totals))


def _resolve_sahacacode_columns(conn) -> Dict[str, str]:
    global _SAHACACODE_COLUMN_CACHE
    if _SAHACACODE_COLUMN_CACHE is not None:
        return _SAHACACODE_COLUMN_CACHE

    def _col_key(name: str) -> str:
        return "".join(ch for ch in name.lower() if ch.isalnum())

    existing: Dict[str, str] = {}
    query = """
    SELECT column_name
    FROM information_schema.columns
    WHERE table_schema = DATABASE() AND table_name = %s
    """
    with conn.cursor() as cur:
        cur.execute(query, ("sahacacode",))
        for row in cur.fetchall():
            col = row[0]
            existing[_col_key(col)] = col

    column_map: Dict[str, str] = {}
    for logical_name, candidates in SAHACACODE_COLUMN_CANDIDATES.items():
        matched: Optional[str] = None
        for candidate in candidates:
            key = _col_key(candidate)
            if key in existing:
                matched = existing[key]
                break
        if matched is None:
            raise KeyError(f"sahacacode 테이블에 {logical_name} 컬럼을 찾을 수 없습니다.")
        column_map[logical_name] = matched

    _SAHACACODE_COLUMN_CACHE = column_map
    return column_map


def fetch_inven_json(url: str) -> Any:
    """지정된 URL에서 JSON 응답을 가져와 파싱."""
    with urllib.request.urlopen(url) as response:
        body = response.read()
        return json.loads(body.decode("utf-8"))


def pick_items(payload: Any) -> List[Dict[str, Any]]:
    """응답에서 전자송품장 목록 리스트를 추출."""
    if isinstance(payload, list):
        return payload

    if isinstance(payload, dict):
        for key in ("body", "data", "list", "items", "result", "rows"):
            candidate = payload.get(key)
            if isinstance(candidate, list):
                return candidate
            if isinstance(candidate, dict):
                for sub_key in ("list", "items", "data", "rows"):
                    nested = candidate.get(sub_key)
                    if isinstance(nested, list):
                        return nested

    return []


def get_detail_list(master_item: Dict[str, Any]) -> List[Dict[str, Any]]:
    for key in ("detailList", "details", "invenDetails", "invenDetailList"):
        candidate = master_item.get(key)
        if isinstance(candidate, list):
            return candidate
    return []


def build_master_row(item: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "invenNo": _clip(item.get("invenNo"), 50),
        "whsalCd": _clip(item.get("whsalCd"), 6),
        "cmpCd": _clip(item.get("cmpCd"), 8),
        "shipType": _clip(item.get("shipType"), 1),
        "shipDate": _clip(item.get("shipDate"), 8),
        "shipName": _clip(item.get("shipName"), 200),
        "shipContact": _clip(item.get("shipContact"), 200),
        "shipDecNo": _clip(item.get("shipDecNo"), 12),
        "shipBankCd": _clip(item.get("shipBankCd"), 255),
        "shipBankName": _clip(item.get("shipBankName"), 255),
        "shipAccNum": _clip(item.get("shipAccNum"), 255),
        "shipAccDep": _clip(item.get("shipAccDep"), 255),
        "tradeType": _clip(item.get("tradeType"), 1),
        "tradeClass": _clip(item.get("tradeClass"), 1),
        "drvName": _clip(item.get("drvName"), 255),
        "drvCall": _clip(item.get("drvCall"), 255),
        "drvCarNo": _clip(item.get("drvCarNo"), 255),
        "drvRate": item.get("drvRate"),
        "drvBankName": _clip(item.get("drvBankName"), 255),
        "drvAccNum": _clip(item.get("drvAccNum"), 255),
        "drvAccDep": _clip(item.get("drvAccDep"), 255),
        "invenState": _clip(item.get("invenState"), 2),
        "registDate": _clip(item.get("registDate"), 14),
        "udtDate": _clip(item.get("udtDate"), 14),
        "chkDate": _clip(item.get("chkDate"), 14),
    }


def build_detail_row(master_inven_no: str, item: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "invenNo": _clip(master_inven_no, 50),
        "invenDetNo": _clip(item.get("invenDetNo"), 22),
        "proName": _clip(item.get("proName"), 255),
        "proDecNo": _clip(item.get("proDecNo"), 255),
        "goodCd": _clip(item.get("goodCd"), 6),
        "pojCd": _clip(item.get("pojCd"), 2),
        "danCd": _clip(item.get("danCd"), 2),
        "lvCd": _clip(item.get("lvCd"), 2),
        "sanCd": _clip(item.get("sanCd"), 6),
        "selfGoodCd": _clip(item.get("selfGoodCd"), 6),
        "selfPojCd": _clip(item.get("selfPojCd"), 6),
        "selfDanCd": _clip(item.get("selfDanCd"), 6),
        "selfLvCd": _clip(item.get("selfLvCd"), 6),
        "selfSanCd": _clip(item.get("selfSanCd"), 6),
        "ecoCd": _clip(item.get("ecoCd"), 1),
        "unitQuantity": item.get("unitQuantity"),
        "shipQuantity": item.get("shipQuantity"),
        "frtQy": item.get("frtQy"),
        "detailNote": item.get("detailNote"),
        "selfGoodNm": _clip(item.get("selfGoodNm"), 255),
        "sugAmt": item.get("sugAmt"),
        "sahaca_amount": ZERO_DECIMAL,
    }


def upsert_master(conn, rows: List[Dict[str, Any]]):
    sql = """
    INSERT INTO electronic_delivery_note_master (
        invenNo, whsalCd, cmpCd, shipType, shipDate, shipName, shipContact,
        shipDecNo, shipBankCd, shipBankName, shipAccNum, shipAccDep,
        tradeType, tradeClass, drvName, drvCall, drvCarNo, drvRate,
        drvBankName, drvAccNum, drvAccDep, invenState, registDate,
        udtDate, chkDate
    )
    VALUES (
        %(invenNo)s, %(whsalCd)s, %(cmpCd)s, %(shipType)s, %(shipDate)s,
        %(shipName)s, %(shipContact)s, %(shipDecNo)s, %(shipBankCd)s,
        %(shipBankName)s, %(shipAccNum)s, %(shipAccDep)s, %(tradeType)s,
        %(tradeClass)s, %(drvName)s, %(drvCall)s, %(drvCarNo)s,
        %(drvRate)s, %(drvBankName)s, %(drvAccNum)s, %(drvAccDep)s,
        %(invenState)s, %(registDate)s, %(udtDate)s, %(chkDate)s
    )
    ON DUPLICATE KEY UPDATE
        whsalCd = IF(COALESCE(chkDate, '') = '', VALUES(whsalCd), whsalCd),
        cmpCd = IF(COALESCE(chkDate, '') = '', VALUES(cmpCd), cmpCd),
        shipType = IF(COALESCE(chkDate, '') = '', VALUES(shipType), shipType),
        shipDate = IF(COALESCE(chkDate, '') = '', VALUES(shipDate), shipDate),
        shipName = IF(COALESCE(chkDate, '') = '', VALUES(shipName), shipName),
        shipContact = IF(COALESCE(chkDate, '') = '', VALUES(shipContact), shipContact),
        shipDecNo = IF(COALESCE(chkDate, '') = '', VALUES(shipDecNo), shipDecNo),
        shipBankCd = IF(COALESCE(chkDate, '') = '', VALUES(shipBankCd), shipBankCd),
        shipBankName = IF(COALESCE(chkDate, '') = '', VALUES(shipBankName), shipBankName),
        shipAccNum = IF(COALESCE(chkDate, '') = '', VALUES(shipAccNum), shipAccNum),
        shipAccDep = IF(COALESCE(chkDate, '') = '', VALUES(shipAccDep), shipAccDep),
        tradeType = IF(COALESCE(chkDate, '') = '', VALUES(tradeType), tradeType),
        tradeClass = IF(COALESCE(chkDate, '') = '', VALUES(tradeClass), tradeClass),
        drvName = IF(COALESCE(chkDate, '') = '', VALUES(drvName), drvName),
        drvCall = IF(COALESCE(chkDate, '') = '', VALUES(drvCall), drvCall),
        drvCarNo = IF(COALESCE(chkDate, '') = '', VALUES(drvCarNo), drvCarNo),
        drvRate = IF(COALESCE(chkDate, '') = '', VALUES(drvRate), drvRate),
        drvBankName = IF(COALESCE(chkDate, '') = '', VALUES(drvBankName), drvBankName),
        drvAccNum = IF(COALESCE(chkDate, '') = '', VALUES(drvAccNum), drvAccNum),
        drvAccDep = IF(COALESCE(chkDate, '') = '', VALUES(drvAccDep), drvAccDep),
        invenState = IF(COALESCE(chkDate, '') = '', VALUES(invenState), invenState),
        registDate = IF(COALESCE(chkDate, '') = '', VALUES(registDate), registDate),
        udtDate = IF(COALESCE(chkDate, '') = '', VALUES(udtDate), udtDate),
        chkDate = IF(COALESCE(chkDate, '') = '', VALUES(chkDate), chkDate)
    """
    with conn.cursor() as cur:
        cur.executemany(sql, rows)


def upsert_detail(conn, rows: List[Dict[str, Any]]):
    sql = """
    INSERT INTO electronic_delivery_note_detail (
        invenNo, invenDetNo, proName, proDecNo, goodCd, pojCd, danCd, lvCd,
        sanCd, selfGoodCd, selfPojCd, selfDanCd, selfLvCd, selfSanCd, ecoCd,
        unitQuantity, shipQuantity, frtQy, detailNote, selfGoodNm, sugAmt,
        sahaca_amount
    )
    VALUES (
        %(invenNo)s, %(invenDetNo)s, %(proName)s, %(proDecNo)s, %(goodCd)s,
        %(pojCd)s, %(danCd)s, %(lvCd)s, %(sanCd)s, %(selfGoodCd)s,
        %(selfPojCd)s, %(selfDanCd)s, %(selfLvCd)s, %(selfSanCd)s,
        %(ecoCd)s, %(unitQuantity)s, %(shipQuantity)s, %(frtQy)s,
        %(detailNote)s, %(selfGoodNm)s, %(sugAmt)s, %(sahaca_amount)s
    )
    ON DUPLICATE KEY UPDATE
        proName = IF(flag_erp_apply = 1, proName, VALUES(proName)),
        proDecNo = IF(flag_erp_apply = 1, proDecNo, VALUES(proDecNo)),
        goodCd = IF(flag_erp_apply = 1, goodCd, VALUES(goodCd)),
        pojCd = IF(flag_erp_apply = 1, pojCd, VALUES(pojCd)),
        danCd = IF(flag_erp_apply = 1, danCd, VALUES(danCd)),
        lvCd = IF(flag_erp_apply = 1, lvCd, VALUES(lvCd)),
        sanCd = IF(flag_erp_apply = 1, sanCd, VALUES(sanCd)),
        selfGoodCd = IF(flag_erp_apply = 1, selfGoodCd, VALUES(selfGoodCd)),
        selfPojCd = IF(flag_erp_apply = 1, selfPojCd, VALUES(selfPojCd)),
        selfDanCd = IF(flag_erp_apply = 1, selfDanCd, VALUES(selfDanCd)),
        selfLvCd = IF(flag_erp_apply = 1, selfLvCd, VALUES(selfLvCd)),
        selfSanCd = IF(flag_erp_apply = 1, selfSanCd, VALUES(selfSanCd)),
        ecoCd = IF(flag_erp_apply = 1, ecoCd, VALUES(ecoCd)),
        unitQuantity = IF(flag_erp_apply = 1, unitQuantity, VALUES(unitQuantity)),
        shipQuantity = IF(flag_erp_apply = 1, shipQuantity, VALUES(shipQuantity)),
        frtQy = IF(flag_erp_apply = 1, frtQy, VALUES(frtQy)),
        detailNote = IF(flag_erp_apply = 1, detailNote, VALUES(detailNote)),
        selfGoodNm = IF(flag_erp_apply = 1, selfGoodNm, VALUES(selfGoodNm)),
        sugAmt = IF(flag_erp_apply = 1, sugAmt, VALUES(sugAmt)),
        sahaca_amount = IF(flag_erp_apply = 1, sahaca_amount, VALUES(sahaca_amount))
    """
    with conn.cursor() as cur:
        cur.executemany(sql, rows)


def insert_api_log(
    conn,
    *,
    flag_success: int,
    ship_date: str,
    page_no: int,
    status: str,
    tot_cnt: int,
    response_content: str,
):
    now = datetime.now()
    sql = """
    INSERT INTO api_log (
        log_date, log_time, flag_success, shipDate, pageNo, status,
        totCnt, response_content
    ) VALUES (
        %s, %s, %s, %s, %s, %s, %s, %s
    )
    """
    with conn.cursor() as cur:
        cur.execute(
            sql,
            (
                now.date(),
                now.time().replace(microsecond=0),
                flag_success,
                ship_date,
                page_no,
                status,
                tot_cnt,
                response_content,
            ),
        )


def log_api_with_conf(db_conf: Dict[str, Any], **kwargs):
    try:
        conn = pymysql.connect(**db_conf)
    except Exception as e:
        logger.error(f"API 로그 기록 DB 연결 실패: {e}")
        return
    try:
        insert_api_log(conn, **kwargs)
        conn.commit()
    except Exception as e:
        logger.error(f"API 로그 기록 실패: {e}")
    finally:
        conn.close()


def main():
    service_key = os.getenv("SERVICE_KEY")
    if not service_key:
        logger.error("환경 변수 SERVICE_KEY가 설정되어 있지 않습니다.")
        return

    ship_date = os.getenv("SHIP_DATE", "20251117")
    page_no = int(os.getenv("PAGE_NO", "1"))
    api_url = build_api_url(service_key, ship_date, page_no)

    db_conf = {
        "host": os.getenv("MYSQL_HOST", "127.0.0.1"),
        "port": int(os.getenv("MYSQL_PORT", "3306")),
        "user": os.getenv("MYSQL_USER", ""),
        "password": os.getenv("MYSQL_PASSWORD", ""),
        "db": os.getenv("MYSQL_DATABASE", ""),
        "charset": "utf8mb4",
        "autocommit": False,
    }

    # API 호출
    try:
        payload = fetch_inven_json(api_url)
        items = pick_items(payload)
        if not items:
            logger.error("응답에서 전자송품장 데이터를 찾지 못했습니다.")
            log_api_with_conf(
                db_conf,
                flag_success=0,
                ship_date=ship_date,
                page_no=page_no,
                status="fail",
                tot_cnt=0,
                response_content=json.dumps(payload, ensure_ascii=False)[:5000],
            )
            return
    except urllib.error.HTTPError as e:
        logger.error(f"HTTP 오류: {e.code} {e.reason}")
        log_api_with_conf(
            db_conf,
            flag_success=0,
            ship_date=ship_date,
            page_no=page_no,
            status="fail",
            tot_cnt=0,
            response_content=str(e),
        )
        return
    except urllib.error.URLError as e:
        logger.error(f"URL 오류: {e.reason}")
        log_api_with_conf(
            db_conf,
            flag_success=0,
            ship_date=ship_date,
            page_no=page_no,
            status="fail",
            tot_cnt=0,
            response_content=str(e),
        )
        return
    except json.JSONDecodeError:
        logger.error("응답을 JSON으로 파싱하지 못했습니다.")
        log_api_with_conf(
            db_conf,
            flag_success=0,
            ship_date=ship_date,
            page_no=page_no,
            status="fail",
            tot_cnt=0,
            response_content="JSON decode error",
        )
        return

    master_rows: List[Dict[str, Any]] = []
    detail_rows: List[Dict[str, Any]] = []
    for master_item in items:
        master_rows.append(build_master_row(master_item))
        for detail_item in get_detail_list(master_item):
            detail_rows.append(build_detail_row(master_item.get("invenNo"), detail_item))

    try:
        conn = pymysql.connect(**db_conf)
    except Exception as e:
        logger.error(f"DB 연결 실패: {e}")
        log_api_with_conf(
            db_conf,
            flag_success=0,
            ship_date=ship_date,
            page_no=page_no,
            status="fail",
            tot_cnt=0,
            response_content=str(e),
        )
        return

    try:
        if master_rows:
            upsert_master(conn, master_rows)
        if detail_rows:
            populate_sahaca_amount(conn, detail_rows)
            upsert_detail(conn, detail_rows)
            update_daily_unloading_cost_total(
                conn, [row.get("invenNo") for row in detail_rows]
            )
        insert_api_log(
            conn,
            flag_success=1,
            ship_date=ship_date,
            page_no=page_no,
            status="success",
            tot_cnt=len(items),
            response_content=json.dumps(payload, ensure_ascii=False)[:5000],
        )
        conn.commit()
        logger.info(f"마스터 {len(master_rows)}건, 디테일 {len(detail_rows)}건 DB 반영 완료.")
    except Exception as e:
        conn.rollback()
        logger.error(f"DB 처리 중 오류: {e}")
        try:
            insert_api_log(
                conn,
                flag_success=0,
                ship_date=ship_date,
                page_no=page_no,
                status="fail",
                tot_cnt=0,
                response_content=str(e),
            )
            conn.commit()
        except Exception as log_err:
            logger.error(f"API 로그 기록 실패: {log_err}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
