import os
import logging
from logging.handlers import TimedRotatingFileHandler, RotatingFileHandler
from datetime import datetime

class LoggerKKI:
    """
    LoggerKKI: 연/월/일 단위 로그 생성, 파일 + 콘솔 출력, 백업 관리
    - logging_interval: "Y"=연, "M"=월, "D"=일
    """

    # 생성자
    def __init__(self, log_dir='logs', logger_name="Logger", logging_interval="D"):
        # 로그 파일을 저장할 디렉토리 생성
        os.makedirs(log_dir, exist_ok=True)

        # 로거 설정
        self.logger = logging.getLogger(logger_name)
        self.logger.setLevel(logging.INFO)
        
        # 기존 핸들러 제거 (중복 방지)
        if self.logger.hasHandlers():
            self.logger.handlers.clear()

        ###################################################################################################################################
        # 로깅 주기가 연도단위일 경우
        ###################################################################################################################################
        if logging_interval == "Y":
            # 포맷터 설정
            formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

            log_file_prefix = os.path.join(log_dir, f"{datetime.now().year}.log")

            # RotatingFileHandler: 연도별 새로운 로그 파일 생성, 백업 파일 5개 유지
            file_handler = RotatingFileHandler(
                filename=log_file_prefix,
                maxBytes=10*1024*1024,
                backupCount=5,
                encoding='utf-8'
            )

        ###################################################################################################################################
        # 로깅 주기가 월단위일 경우
        ###################################################################################################################################
        elif logging_interval == "M":
            # 포맷터 설정
            formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

            log_file_prefix = os.path.join(log_dir, f"{datetime.now().strftime('%Y%m')}.log")

            # TimedRotatingFileHandler: 월별 새로운 로그 파일 생성, 백업 파일 12개 유지
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
        # 로깅 주기가 일단위일 경우
        ###################################################################################################################################
        else:
            # 포맷터 설정
            formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%H:%M:%S')

            log_file_prefix = os.path.join(log_dir, f"{datetime.now().strftime('%Y%m%d')}.log")

            # TimedRotatingFileHandler: 매일 자정마다 새로운 파일 생성, 백업 파일 30개까지 유지
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

        # 콘솔에도 로그를 출력하고 싶다면 StreamHandler 추가
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)
        self.logger.addHandler(stream_handler)


    def get_logger(self):
        return self.logger

# ===== 사용 예시 =====

# from logger_kki import LoggerKKI

# logger = LoggerKKI(logging_interval="Y").get_logger()

# logger.info("연 단위 로그 기록 시작")

# logger = LoggerKKI(logging_interval="M").get_logger()

# logger.info("월 단위 로그 기록 시작")

# logger = LoggerKKI(logging_interval="D").get_logger()

# logger.info("일 단위 로그 기록 시작")