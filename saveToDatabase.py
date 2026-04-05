import json
import logging
import re
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
CHATTING_DATABASE_NAME = "taskrit-chatting"
CHATTING_COLLECTION_NAME = "users"
TEST_USER_ID_REGEX = r"^test_user_\d+$"
TEST_NICKNAME_REGEX = r"^TestUser\d+$"

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
        self.chatting_db = None
        self.users_collection = None
        self.teaming_collection = None
        self.chatting_user_collection = None
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
            self.chatting_db = self.client[CHATTING_DATABASE_NAME]
            self.users_collection = self.db["users"]
            self.teaming_collection = self.db["teaming"]
            self.chatting_user_collection = self.chatting_db[CHATTING_COLLECTION_NAME]
            logger.info(
                f"MongoDB 연결 성공: {DATABASE_NAME} - users, teaming / "
                f"{CHATTING_DATABASE_NAME} - {CHATTING_COLLECTION_NAME} 컬렉션"
            )
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

    def is_generated_test_account(self, account: Dict) -> bool:
        """이 스크립트로 생성된 테스트 계정인지 확인합니다"""
        user_id = account.get("user_id") or ""
        nickname = account.get("nickname") or ""
        return bool(
            re.match(TEST_USER_ID_REGEX, user_id)
            or re.match(TEST_NICKNAME_REGEX, nickname)
        )

    def get_generated_test_uuids(self, accounts: List[Dict]) -> List[str]:
        """이 스크립트로 생성된 테스트 계정의 UUID 목록을 수집합니다"""
        uuids = []
        for acc in accounts:
            if not self.is_generated_test_account(acc):
                continue
            user_uuid = acc.get("uuid")
            if user_uuid:
                uuids.append(user_uuid)
        return uuids
    
    def save_accounts(self, accounts: List[Dict]) -> int:
        """사용자 정보를 users 컬렉션에 저장합니다"""
        if self.users_collection is None:
            logger.error("MongoDB users 컬렉션이 연결되지 않았습니다")
            return 0

        # 이 스크립트로 생성된 테스트 계정만 처리
        test_accounts = [acc for acc in accounts if self.is_generated_test_account(acc)]
        if not test_accounts:
            logger.error("생성 규칙(TestUser/test_user_)에 맞는 테스트 계정이 없습니다")
            return 0
        
        # 올바른 형식으로 변환
        cleaned_accounts = []
        for acc in test_accounts:
            cleaned = self.clean_account_data(acc)
            if cleaned is not None:
                cleaned_accounts.append(cleaned)
        
        if not cleaned_accounts:
            logger.error("변환된 계정 정보가 없습니다")
            return 0
        
        try:
            # 기존 테스트 데이터만 삭제
            delete_result = self.users_collection.delete_many({
                "$or": [
                    {"user_id": {"$regex": TEST_USER_ID_REGEX}},
                    {"nickname": {"$regex": TEST_NICKNAME_REGEX}}
                ]
            })
            logger.info(
                f"기존 테스트 users 레코드 {delete_result.deleted_count}개를 삭제했습니다"
            )
            
            # 새 데이터 삽입
            insert_result = self.users_collection.insert_many(cleaned_accounts)
            logger.info(f"✓ {len(insert_result.inserted_ids)}개의 사용자 데이터가 users 컬렉션에 저장되었습니다")
            
            return len(insert_result.inserted_ids)
        except Exception as e:
            logger.error(f"users 컬렉션 저장 중 오류 발생: {e}")
            return 0
    
    def build_account_metadata(self, account: Dict) -> Dict:
        """계정 정보를 teaming 컬렉션 형식으로 변환합니다"""
        try:
            account_data = {
                "user_uuid": account.get("uuid", str(uuid4())),
                "type": "human",
                "elo": 1000,
                "availability": True,
                "cost": 0
            }
            return account_data
        except Exception as e:
            logger.error(f"계정 메타데이터 변환 오류: {e}")
            return None
    
    def save_account_metadata(self, accounts: List[Dict]) -> int:
        """팀 매칭 정보를 teaming 컬렉션에 저장합니다"""
        if self.teaming_collection is None:
            logger.error("MongoDB teaming 컬렉션이 연결되지 않았습니다")
            return 0

        test_accounts = [acc for acc in accounts if self.is_generated_test_account(acc)]
        if not test_accounts:
            logger.error("생성 규칙(TestUser/test_user_)에 맞는 테스트 계정이 없습니다")
            return 0
        
        # accounts 형식으로 변환
        account_metadata = []
        for acc in test_accounts:
            metadata = self.build_account_metadata(acc)
            if metadata is not None:
                account_metadata.append(metadata)
        
        if not account_metadata:
            logger.error("변환된 계정 메타데이터가 없습니다")
            return 0
        
        try:
            # 이번 파일로 생성된 테스트 계정 UUID에 해당하는 데이터만 삭제
            test_uuids = self.get_generated_test_uuids(test_accounts)
            delete_result = self.teaming_collection.delete_many({
                "user_uuid": {"$in": test_uuids}
            })
            logger.info(
                f"기존 테스트 teaming 레코드 {delete_result.deleted_count}개를 삭제했습니다"
            )
            
            # 새 데이터 삽입
            insert_result = self.teaming_collection.insert_many(account_metadata)
            logger.info(f"✓ {len(insert_result.inserted_ids)}개의 계정 메타데이터가 teaming 컬렉션에 저장되었습니다")
            
            return len(insert_result.inserted_ids)
        except Exception as e:
            logger.error(f"teaming 컬렉션 저장 중 오류 발생: {e}")
            return 0

    def build_chatting_user_data(self, account: Dict) -> Dict:
        """계정 정보를 taskrit-chatting.users 컬렉션 형식으로 변환합니다"""
        try:
            chatting_user_data = {
                "user_uuid": account.get("uuid", str(uuid4())),
                "nickname": account.get("nickname"),
                "user_id": account.get("user_id")
            }
            return chatting_user_data
        except Exception as e:
            logger.error(f"chatting user 데이터 변환 오류: {e}")
            return None

    def save_chatting_users(self, accounts: List[Dict]) -> int:
        """사용자 정보를 taskrit-chatting.users 컬렉션에 저장합니다"""
        if self.chatting_user_collection is None:
            logger.error("MongoDB taskrit-chatting.users 컬렉션이 연결되지 않았습니다")
            return 0

        test_accounts = [acc for acc in accounts if self.is_generated_test_account(acc)]
        if not test_accounts:
            logger.error("생성 규칙(TestUser/test_user_)에 맞는 테스트 계정이 없습니다")
            return 0

        chatting_users = []
        for acc in test_accounts:
            chatting_user = self.build_chatting_user_data(acc)
            if chatting_user is not None:
                chatting_users.append(chatting_user)

        if not chatting_users:
            logger.error("변환된 chatting user 데이터가 없습니다")
            return 0

        try:
            delete_result = self.chatting_user_collection.delete_many({
                "$or": [
                    {"user_id": {"$regex": TEST_USER_ID_REGEX}},
                    {"nickname": {"$regex": TEST_NICKNAME_REGEX}}
                ]
            })
            logger.info(
                f"기존 테스트 taskrit-chatting.users 레코드 {delete_result.deleted_count}개를 삭제했습니다"
            )

            insert_result = self.chatting_user_collection.insert_many(chatting_users)
            logger.info(
                "✓ "
                f"{len(insert_result.inserted_ids)}개의 사용자 데이터가 "
                "taskrit-chatting.users 컬렉션에 저장되었습니다"
            )

            return len(insert_result.inserted_ids)
        except Exception as e:
            logger.error(f"taskrit-chatting.users 컬렉션 저장 중 오류 발생: {e}")
            return 0
    
    def verify_save(self) -> bool:
        """저장된 데이터를 검증합니다"""
        try:
            users_count = self.users_collection.count_documents({})
            teaming_count = self.teaming_collection.count_documents({})
            chatting_users_count = self.chatting_user_collection.count_documents({})
            logger.info(
                "✓ "
                f"users 컬렉션: {users_count}개 / "
                f"teaming 컬렉션: {teaming_count}개 / "
                f"taskrit-chatting.users 컬렉션: {chatting_users_count}개"
            )
            
            # users 샘플 데이터 확인
            users_sample = self.users_collection.find_one()
            if users_sample:
                users_info = {
                    "user_uuid": users_sample.get("user_uuid"),
                    "user_id": users_sample.get("user_id"),
                    "nickname": users_sample.get("nickname"),
                    "profile_bio": users_sample.get("profile_bio", "")[:50]
                }
                logger.info(f"users 샘플: {json.dumps(users_info, ensure_ascii=False)}")
            
            # teaming 샘플 데이터 확인
            teaming_sample = self.teaming_collection.find_one()
            if teaming_sample:
                teaming_info = {
                    "user_uuid": teaming_sample.get("user_uuid"),
                    "type": teaming_sample.get("type"),
                    "elo": teaming_sample.get("elo"),
                    "availability": teaming_sample.get("availability"),
                    "cost": teaming_sample.get("cost")
                }
                logger.info(f"teaming 샘플: {json.dumps(teaming_info, ensure_ascii=False)}")

            # taskrit-chatting.users 샘플 데이터 확인
            chatting_user_sample = self.chatting_user_collection.find_one()
            if chatting_user_sample:
                chatting_user_info = {
                    "user_uuid": chatting_user_sample.get("user_uuid"),
                    "nickname": chatting_user_sample.get("nickname"),
                    "user_id": chatting_user_sample.get("user_id")
                }
                logger.info(
                    f"taskrit-chatting.users 샘플: {json.dumps(chatting_user_info, ensure_ascii=False)}"
                )
            
            return users_count > 0 and teaming_count > 0 and chatting_users_count > 0
        except Exception as e:
            logger.error(f"데이터 검증 중 오류: {e}")
            return False


def main():
    """메인 함수"""
    logger.info("=" * 60)
    logger.info("사용자 데이터 MongoDB 저장 시작 (users, teaming, taskrit-chatting.users 컬렉션)")
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
    
    # users 컬렉션에 사용자 정보 저장
    logger.info("\n[1/3] users 컬렉션에 사용자 정보 저장 중...")
    saved_count = saver.save_accounts(accounts)
    
    # teaming 컬렉션에 팀 매칭 정보 저장
    logger.info("[2/3] teaming 컬렉션에 팀 매칭 정보 저장 중...")
    metadata_count = saver.save_account_metadata(accounts)

    # taskrit-chatting.users 컬렉션에 사용자 정보 저장
    logger.info("[3/3] taskrit-chatting.users 컬렉션에 사용자 정보 저장 중...")
    chatting_saved_count = saver.save_chatting_users(accounts)
    
    # 데이터 검증
    if saved_count > 0 and metadata_count > 0 and chatting_saved_count > 0:
        logger.info("\n데이터 검증 중...")
        saver.verify_save()
    
    saver.disconnect()
    
    
    logger.info("=" * 60)
    logger.info("프로세스 완료")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
