# =============================================================
# [ 운영 원칙 ]
# - 주석은 UTF-8 기준으로 읽기 쉽게 유지하고, 로그 저장 경로와 회전 정책을 함께 적는다.
# - log_dir는 항상 미리 생성하고, 핸들러가 중복 등록되지 않게 관리한다.
# - SERVICE_KEY, DB 계정 등 비밀값은 로거 밖(.env 등)에서 관리한다.
# - 로그 회전 정책(Y/M/D)을 바꾸면 README나 주석도 즉시 같이 갱신한다.
# - 장애 확인 시 logs/ 파일과 콘솔 출력 둘 다 먼저 본다.
# =============================================================
# [ 흐름도 요약 ]
# - LoggerKKI 생성 시 log_dir를 만든다.
# - logging_interval(Y/M/D)에 맞춰 파일 핸들러와 포맷터를 준비한다.
# - 기존 핸들러를 비워 중복 출력을 막는다.
# - 파일 핸들러와 콘솔(Stream) 핸들러를 로거에 등록한다.
# - get_logger()가 설정 완료된 logger 인스턴스를 반환한다.
# =============================================================

import logging
import os
from datetime import datetime
from logging.handlers import RotatingFileHandler, TimedRotatingFileHandler

class LoggerKKI:
    """
    LoggerKKI: 연도/월/일 단위 로그 파일 생성, 파일 + 콘솔 출력, 백업 관리
    - logging_interval: "Y"=연도별, "M"=월별, "D"=일별
    """

    # 생성자
    def __init__(self, log_dir='logs', logger_name="Logger", logging_interval="D"):
        # 로그 파일을 저장할 디렉터리 생성
        os.makedirs(log_dir, exist_ok=True)

        # 로거 설정
        self.logger = logging.getLogger(logger_name)
        self.logger.setLevel(logging.INFO)
        
        # 기존 핸들러 제거 (중복 방지)
        if self.logger.hasHandlers():
            self.logger.handlers.clear()

        ###################################################################################################################################
        # 로그 주기가 연도 단위인 경우
        ###################################################################################################################################
        if logging_interval == "Y":
            # 포맷 설정
            formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

            log_file_prefix = os.path.join(log_dir, f"{datetime.now().year}.log")

            # 연도별 로그 파일을 만들고, 크기 기준으로 백업 5개까지 유지
            file_handler = RotatingFileHandler(
                filename=log_file_prefix,
                maxBytes=10*1024*1024,
                backupCount=5,
                encoding='utf-8'
            )

        ###################################################################################################################################
        # 로그 주기가 월 단위인 경우
        ###################################################################################################################################
        elif logging_interval == "M":
            # 포맷 설정
            formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

            log_file_prefix = os.path.join(log_dir, f"{datetime.now().strftime('%Y%m')}.log")

            # 날짜 변경 시 새 로그 파일로 넘기고, 월별 백업 12개까지 유지
            file_handler = TimedRotatingFileHandler(
                filename=log_file_prefix,
                when='midnight',
                interval=1,
                backupCount=12,
                encoding='utf-8',
                utc=False
            )
            file_handler.suffix = "%Y-%m"

        ###################################################################################################################################
        # 기본값: 로그 주기가 일 단위인 경우
        ###################################################################################################################################
        else:
            # 포맷 설정
            formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%H:%M:%S')

            log_file_prefix = os.path.join(log_dir, f"{datetime.now().strftime('%Y%m%d')}.log")

            # 자정마다 새 파일로 넘기고, 일별 백업 90개까지 유지
            file_handler = TimedRotatingFileHandler(
                filename=log_file_prefix,
                when='midnight',
                interval=1,
                backupCount=90,
                encoding='utf-8',
                utc=False
            )
            file_handler.suffix = "%Y-%m-%d"

        ###################################################################################################################################

        file_handler.setFormatter(formatter)
        self.logger.addHandler(file_handler)

        # 콘솔에도 같은 형식으로 로그를 출력
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)
        self.logger.addHandler(stream_handler)


    def get_logger(self):
        return self.logger

# ===== 사용 예시 =====

# from logger_kki import LoggerKKI

# logger = LoggerKKI(logging_interval="Y").get_logger()

# logger.info("연도 단위 로그 기록 시작")

# logger = LoggerKKI(logging_interval="M").get_logger()

# logger.info("월 단위 로그 기록 시작")

# logger = LoggerKKI(logging_interval="D").get_logger()

# logger.info("일 단위 로그 기록 시작")
