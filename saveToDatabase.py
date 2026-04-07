import json
import logging
import re
from typing import List, Dict
import hashlib
import hmac
import requests
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
TEAMING_API_BASE = os.getenv("TEAMING_API_BASE", "http://localhost:3002").rstrip("/")
TEAMING_HMAC_KEY = os.getenv("HMAC_KEY", "").strip()
TEAMING_REQUEST_TIMEOUT = int(os.getenv("TEAMING_REQUEST_TIMEOUT", "10"))
TEAMING_SKIP_AI = os.getenv("TEAMING_SKIP_AI", "true").lower() == "true"
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
        self.teaming_api_base = TEAMING_API_BASE
        self.teaming_hmac_key = TEAMING_HMAC_KEY
        self.teaming_timeout = TEAMING_REQUEST_TIMEOUT
        self.teaming_skip_ai = TEAMING_SKIP_AI
        self.http_session = requests.Session()
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
        self.http_session.close()
        if self.client is not None:
            self.client.close()
            logger.info("MongoDB 연결 종료")

    def generate_hmac(self, account_id: str) -> str:
        """teaming API 요청용 HMAC 서명을 생성합니다"""
        # 개발 환경에서는 HMAC_KEY가 비어 있을 수 있다.
        # 이 경우 빈 키로 서명하고, 서버 키와 불일치하면 API에서 403을 반환한다.
        key = self.teaming_hmac_key or ""
        return hmac.new(
            key.encode('utf-8'),
            account_id.encode('utf-8'),
            hashlib.sha256,
        ).hexdigest()

    @staticmethod
    def _response_detail(response) -> str:
        """HTTP 응답에서 에러 상세를 추출합니다"""
        try:
            payload = response.json()
            return payload.get("detail") or payload.get("error") or response.text
        except Exception:
            return response.text

    def _build_teaming_payload(self, account: Dict) -> Dict:
        """teaming API 요청 payload를 생성합니다"""
        ability_text = (account.get("profile") or "").strip()
        if not ability_text:
            fallback_name = (account.get("nickname") or account.get("user_id") or "테스트 계정").strip()
            ability_text = f"{fallback_name} 프로필"

        return {
            "userId": account.get("user_id"),
            "nickname": account.get("nickname"),
            "abilityText": ability_text,
            "cost": 0,
            "skipAi": self.teaming_skip_ai,
        }

    def _delete_teaming_account_via_api(self, account_id: str) -> bool:
        """teaming API를 통해 계정을 삭제합니다(없으면 성공으로 간주)."""
        signature = self.generate_hmac(account_id)
        delete_url = f"{self.teaming_api_base}/Account/{account_id}"

        try:
            response = self.http_session.delete(
                delete_url,
                params={"hmac": signature},
                timeout=self.teaming_timeout,
            )
        except requests.RequestException as e:
            logger.error(f"teaming API 삭제 요청 실패({account_id}): {e}")
            return False

        if response.status_code in (200, 204, 404):
            return True

        logger.error(
            f"teaming API 삭제 실패({account_id}): "
            f"status={response.status_code}, detail={self._response_detail(response)}"
        )
        return False

    def _create_teaming_account_via_api(self, account: Dict) -> bool:
        """teaming API를 통해 계정을 생성합니다"""
        account_id = account.get("uuid", str(uuid4()))
        signature = self.generate_hmac(account_id)

        create_payload = {
            "accountId": account_id,
            "type": "human",
            **self._build_teaming_payload(account),
            "hmac": signature,
        }

        create_url = f"{self.teaming_api_base}/Account"

        try:
            response = self.http_session.post(
                create_url,
                json=create_payload,
                timeout=self.teaming_timeout,
            )
        except requests.RequestException as e:
            logger.error(f"teaming API 생성 요청 실패({account_id}): {e}")
            return False

        if response.status_code in (200, 201):
            return True

        logger.error(
            f"teaming API 생성 실패({account_id}): "
            f"status={response.status_code}, detail={self._response_detail(response)}"
        )
        return False
    
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
    
    def save_account_metadata(self, accounts: List[Dict]) -> int:
        """팀 매칭 정보를 teaming API 경유로 저장합니다"""
        if not self.teaming_hmac_key:
            logger.warning(
                "HMAC_KEY가 설정되지 않아 빈 키로 서명합니다. "
                "서버 HMAC_KEY와 다르면 403이 발생할 수 있습니다."
            )

        test_accounts = [acc for acc in accounts if self.is_generated_test_account(acc)]
        if not test_accounts:
            logger.error("생성 규칙(TestUser/test_user_)에 맞는 테스트 계정이 없습니다")
            return 0

        # 기존 테스트 계정은 API로 삭제 후 재생성하여 벡터/구성요소까지 재빌드한다.
        deleted_count = 0
        created_count = 0

        for acc in test_accounts:
            account_id = acc.get("uuid")
            if not account_id:
                logger.warning(f"uuid가 없는 계정은 건너뜁니다: user_id={acc.get('user_id')}")
                continue

            if self._delete_teaming_account_via_api(account_id):
                deleted_count += 1

            if self._create_teaming_account_via_api(acc):
                created_count += 1

        logger.info(
            f"teaming API 처리 완료: 삭제 시도 성공 {deleted_count}건, 생성 성공 {created_count}건"
        )
        return created_count

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
    
    # teaming API를 거쳐 팀 매칭 정보 저장
    logger.info("[2/3] teaming API를 통해 팀 매칭 정보 저장 중...")
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
