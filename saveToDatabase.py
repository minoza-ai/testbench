import json
import logging
from typing import List, Dict
from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError
from dotenv import load_dotenv
import os
import bcrypt
from datetime import datetime
from uuid import uuid4

# 환경 변수 로드
load_dotenv()

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('db_save.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# MongoDB 설정
MONGODB_URI = os.getenv("MONGODB_URI", "mongodb://localhost:27017")
DATABASE_NAME = "taskrit"
COLLECTION_NAME = "users"

# 입력 파일
INPUT_FILE = "created_accounts.json"
DEFAULT_PASSWORD = "1234"  # create_accounts.py에서 사용한 기본 비밀번호


class DatabaseSaver:
    """MongoDB에 데이터를 저장하는 클래스"""
    
    def __init__(self, mongodb_uri: str):
        """MongoDB 연결 초기화"""
        self.mongodb_uri = mongodb_uri
        self.client = None
        self.db = None
        self.collection = None
        self.hashed_password = None  # 미리 계산한 해싱 비밀번호
    
    def hash_password(self):
        """비밀번호를 bcrypt로 해싱합니다"""
        if self.hashed_password is None:
            password_bytes = DEFAULT_PASSWORD.encode('utf-8')
            salt = bcrypt.gensalt(rounds=12)
            self.hashed_password = bcrypt.hashpw(password_bytes, salt).decode('utf-8')
            logger.info(f"비밀번호 해싱 완료: {self.hashed_password[:20]}...")
        return self.hashed_password
        
    def connect(self) -> bool:
        """MongoDB에 연결합니다"""
        try:
            self.client = MongoClient(self.mongodb_uri, serverSelectionTimeoutMS=5000)
            # 연결 확인
            self.client.admin.command('ping')
            self.db = self.client[DATABASE_NAME]
            self.collection = self.db[COLLECTION_NAME]
            logger.info(f"MongoDB 연결 성공: {DATABASE_NAME}/{COLLECTION_NAME}")
            return True
        except (ServerSelectionTimeoutError, Exception) as e:
            logger.error(f"MongoDB 연결 실패: {e}")
            return False
    
    def disconnect(self):
        """MongoDB 연결을 종료합니다"""
        if self.client is not None:
            self.client.close()
            logger.info("MongoDB 연결 종료")
    
    def load_accounts(self) -> List[Dict]:
        """JSON 파일에서 계정 정보를 로드합니다"""
        try:
            with open(INPUT_FILE, 'r', encoding='utf-8') as f:
                accounts = json.load(f)
            logger.info(f"{INPUT_FILE}에서 {len(accounts)}개의 계정 정보를 로드했습니다")
            return accounts
        except FileNotFoundError:
            logger.error(f"{INPUT_FILE} 파일을 찾을 수 없습니다")
            return []
        except json.JSONDecodeError as e:
            logger.error(f"JSON 파싱 오류: {e}")
            return []
    
    def clean_account_data(self, account: Dict) -> Dict:
        """계정 정보를 정상적인 사용자 데이터 형식으로 변환합니다"""
        try:
            # timestamp를 Unix 타임스탬프로 변환
            timestamp_str = account.get("timestamp", "")
            if timestamp_str:
                # ISO 형식의 timestamp를 datetime으로 파싱
                dt = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
                unix_timestamp = int(dt.timestamp())
            else:
                unix_timestamp = int(datetime.now().timestamp())
            
            user_data = {
                "user_uuid": account.get("uuid", str(uuid4())),
                "user_id": account.get("user_id"),
                "nickname": account.get("nickname"),
                "password": self.hash_password(),
                "profile_bio": account.get("profile", ""),
                "capabilities": [],
                "wallet_address": None,
                "otp_enabled": False,
                "otp_secret": None,
                "otp_pending_secret": None,
                "created_at": unix_timestamp,
                "updated_at": unix_timestamp,
                "deleted_at": None,
                "__v": 0
            }
            return user_data
        except Exception as e:
            logger.error(f"계정 데이터 변환 오류: {e}")
            return None
    
    def save_accounts(self, accounts: List[Dict]) -> int:
        """계정 정보를 MongoDB에 저장합니다"""
        if self.collection is None:
            logger.error("MongoDB 컬렉션이 연결되지 않았습니다")
            return 0
        
        # 올바른 형식으로 변환
        cleaned_accounts = []
        for acc in accounts:
            cleaned = self.clean_account_data(acc)
            if cleaned is not None:
                cleaned_accounts.append(cleaned)
        
        if not cleaned_accounts:
            logger.error("변환된 계정 정보가 없습니다")
            return 0
        
        try:
            # 기존 데이터 삭제
            delete_result = self.collection.delete_many({})
            logger.info(f"기존 {delete_result.deleted_count}개 레코드를 삭제했습니다")
            
            # 새 데이터 삽입
            insert_result = self.collection.insert_many(cleaned_accounts)
            logger.info(f"✓ {len(insert_result.inserted_ids)}개의 사용자 데이터가 MongoDB에 저장되었습니다")
            
            return len(insert_result.inserted_ids)
        except Exception as e:
            logger.error(f"데이터 저장 중 오류 발생: {e}")
            return 0
    
    def verify_save(self) -> bool:
        """저장된 데이터를 검증합니다"""
        try:
            count = self.collection.count_documents({})
            logger.info(f"MongoDB에 저장된 사용자 수: {count}")
            
            # 샘플 데이터 확인
            sample = self.collection.find_one()
            if sample:
                sample_info = {
                    "user_uuid": sample.get("user_uuid"),
                    "user_id": sample.get("user_id"),
                    "nickname": sample.get("nickname"),
                    "profile_bio": sample.get("profile_bio", "")[:50],  # 처음 50자
                    "created_at": sample.get("created_at")
                }
                logger.info(f"샘플 데이터: {json.dumps(sample_info, ensure_ascii=False)}")
                return True
            return False
        except Exception as e:
            logger.error(f"데이터 검증 중 오류: {e}")
            return False


def main():
    """메인 함수"""
    logger.info("=" * 60)
    logger.info("사용자 데이터 MongoDB 저장 시작")
    logger.info("=" * 60)
    
    saver = DatabaseSaver(MONGODB_URI)
    
    # 비밀번호 미리 해싱
    logger.info(f"비밀번호 해싱 중... (기본 비밀번호: {DEFAULT_PASSWORD})")
    saver.hash_password()
    
    # MongoDB 연결
    if not saver.connect():
        logger.error("MongoDB 연결에 실패했습니다. 프로세스를 종료합니다.")
        return
    
    # 계정 정보 로드
    accounts = saver.load_accounts()
    if not accounts:
        logger.error("로드할 계정 정보가 없습니다.")
        saver.disconnect()
        return
    
    # 계정 정보 저장
    saved_count = saver.save_accounts(accounts)
    
    # 데이터 검증
    if saved_count > 0:
        saver.verify_save()
    
    saver.disconnect()
    
    logger.info("=" * 60)
    logger.info("프로세스 완료")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
