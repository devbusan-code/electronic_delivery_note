# Electronic Delivery Note 수집기

Agromarket 전자송품장 오픈 API에서 데이터를 가져와 MySQL에 저장하고, 상하차비를 계산해 합계 테이블까지 반영하는 스크립트입니다. `main.py` 하나로 동작하며, 로그는 `logs/` 디렉터리에 일자별로 남습니다.

## 요구사항
- Python 3.12+
- MySQL 접근 권한 및 대상 테이블(`electronic_delivery_note_master`, `electronic_delivery_note_detail`, `sahacacode`, `chulcode_matching`, `daily_unloading_cost_total`, `api_log`)
- 패키지: `pymysql`, `python-dotenv` (uv 또는 pip로 설치)

## 설치
```bash
# uv 권장
uv sync
# 또는
uv pip install pymysql python-dotenv
```

## 환경 변수(.env)
`readme.txt`에 있던 값 예시를 옮겨 사용합니다.
```
SERVICE_KEY=발급받은_서비스키
MYSQL_HOST=localhost
MYSQL_PORT=mysql 포트번호
MYSQL_USER=사용자
MYSQL_PASSWORD=비밀번호
MYSQL_DATABASE=DB명
```
- 서비스 키는 Agromarket 관리 페이지의 전자송품장 API(`https://at.agromarket.kr/admin/elecInvoice/api.do`)에서 발급된 값을 넣습니다.

## 실행 방법
```bash
uv run main.py 20251125
```
- 날짜 인자는 `YYYYMMDD` 형식입니다(구분자 없이 8자리).
- `SERVICE_KEY`가 없거나 형식이 잘못되면 바로 종료합니다.

## 동작 요약
- API를 페이지 단위로 호출해 마스터/디테일을 각각 업서트합니다.
- 숫자형 필드는 빈 문자열과 콤마를 정리하고, DB 숫자 타입에 맞지 않는 값은 `NULL`로 저장합니다.
- `sahacacode` 테이블을 조회해 단위수량별 사하차비를 계산 후 디테일에 반영합니다.
- `chulcode_matching`을 기준으로 일별 하차비 합계를 `daily_unloading_cost_total`에 업서트합니다.
- 성공/실패를 `api_log` 테이블에 기록합니다.

## 로그
- `logger_kki.py`가 일자별 로그를 `logs/`에 저장합니다(콘솔에도 출력).
- 필요 시 `LoggerKKI(logging_interval="Y"|"M"|"D")`로 로그 회전 주기를 조정할 수 있습니다.
