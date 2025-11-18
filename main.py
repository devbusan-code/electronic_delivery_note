# install dependencies:
# uv add python-dotenv pymysql

import json
import os
import urllib.error
import urllib.request
from datetime import datetime
from typing import Any, Dict, List

import pymysql
from dotenv import load_dotenv
from logger_kki import LoggerKKI


logger = LoggerKKI(logging_interval="Y").get_logger()

# .env 파일 로드
load_dotenv()


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
        whsalCd=VALUES(whsalCd),
        cmpCd=VALUES(cmpCd),
        shipType=VALUES(shipType),
        shipDate=VALUES(shipDate),
        shipName=VALUES(shipName),
        shipContact=VALUES(shipContact),
        shipDecNo=VALUES(shipDecNo),
        shipBankCd=VALUES(shipBankCd),
        shipBankName=VALUES(shipBankName),
        shipAccNum=VALUES(shipAccNum),
        shipAccDep=VALUES(shipAccDep),
        tradeType=VALUES(tradeType),
        tradeClass=VALUES(tradeClass),
        drvName=VALUES(drvName),
        drvCall=VALUES(drvCall),
        drvCarNo=VALUES(drvCarNo),
        drvRate=VALUES(drvRate),
        drvBankName=VALUES(drvBankName),
        drvAccNum=VALUES(drvAccNum),
        drvAccDep=VALUES(drvAccDep),
        invenState=VALUES(invenState),
        registDate=VALUES(registDate),
        udtDate=VALUES(udtDate),
        chkDate=VALUES(chkDate)
    """
    with conn.cursor() as cur:
        cur.executemany(sql, rows)


def upsert_detail(conn, rows: List[Dict[str, Any]]):
    sql = """
    INSERT INTO electronic_delivery_note_detail (
        invenNo, invenDetNo, proName, proDecNo, goodCd, pojCd, danCd, lvCd,
        sanCd, selfGoodCd, selfPojCd, selfDanCd, selfLvCd, selfSanCd, ecoCd,
        unitQuantity, shipQuantity, frtQy, detailNote, selfGoodNm, sugAmt
    )
    VALUES (
        %(invenNo)s, %(invenDetNo)s, %(proName)s, %(proDecNo)s, %(goodCd)s,
        %(pojCd)s, %(danCd)s, %(lvCd)s, %(sanCd)s, %(selfGoodCd)s,
        %(selfPojCd)s, %(selfDanCd)s, %(selfLvCd)s, %(selfSanCd)s,
        %(ecoCd)s, %(unitQuantity)s, %(shipQuantity)s, %(frtQy)s,
        %(detailNote)s, %(selfGoodNm)s, %(sugAmt)s
    )
    ON DUPLICATE KEY UPDATE
        proName=VALUES(proName),
        proDecNo=VALUES(proDecNo),
        goodCd=VALUES(goodCd),
        pojCd=VALUES(pojCd),
        danCd=VALUES(danCd),
        lvCd=VALUES(lvCd),
        sanCd=VALUES(sanCd),
        selfGoodCd=VALUES(selfGoodCd),
        selfPojCd=VALUES(selfPojCd),
        selfDanCd=VALUES(selfDanCd),
        selfLvCd=VALUES(selfLvCd),
        selfSanCd=VALUES(selfSanCd),
        ecoCd=VALUES(ecoCd),
        unitQuantity=VALUES(unitQuantity),
        shipQuantity=VALUES(shipQuantity),
        frtQy=VALUES(frtQy),
        detailNote=VALUES(detailNote),
        selfGoodNm=VALUES(selfGoodNm),
        sugAmt=VALUES(sugAmt)
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
        print(f"API 로그 기록 DB 연결 실패: {e}")
        return
    try:
        insert_api_log(conn, **kwargs)
        conn.commit()
    except Exception as e:
        print(f"API 로그 기록 실패: {e}")
    finally:
        conn.close()


def main():
    service_key = os.getenv("SERVICE_KEY")
    if not service_key:
        print("환경 변수 SERVICE_KEY가 설정되어 있지 않습니다.")
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
            print("응답에서 전자송품장 데이터를 찾지 못했습니다.")
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
        print(f"HTTP 오류: {e.code} {e.reason}")
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
        print(f"URL 오류: {e.reason}")
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
        print("응답을 JSON으로 파싱하지 못했습니다.")
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
        print(f"DB 연결 실패: {e}")
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
            upsert_detail(conn, detail_rows)
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
        print(f"마스터 {len(master_rows)}건, 디테일 {len(detail_rows)}건 DB 반영 완료.")
    except Exception as e:
        conn.rollback()
        print(f"DB 처리 중 오류: {e}")
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
            print(f"API 로그 기록 실패: {log_err}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()