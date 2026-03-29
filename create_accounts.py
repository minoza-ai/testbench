#!/usr/bin/env python3
"""
Taskrit 플랫폼 테스트 계정 자동 생성 스크립트
profile.txt의 프로필 설명 문구를 이용해 1000개 테스트 계정을 생성합니다.
"""

import requests
import hashlib
import json
import time
from typing import Optional, Dict, List
from pathlib import Path
import asyncio
import aiohttp
from datetime import datetime
import logging

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('account_creation.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# API 설정
API_BASE_URL = "http://localhost:3000"
REGISTER_ENDPOINT = f"{API_BASE_URL}/user/register"
HASH_PASSWORD_ENDPOINT = f"{API_BASE_URL}/test/hash-password"

# 설정
BATCH_SIZE = 50  # 동시 처리 수
PROFILE_FILE = "profile.txt"
OUTPUT_FILE = "created_accounts.json"
ERROR_LOG_FILE = "creation_errors.log"

# 기본 암호
DEFAULT_PASSWORD = "1234"


def sha256_hash(text: str) -> str:
    """문자열을 SHA256으로 해싱합니다."""
    return hashlib.sha256(text.encode()).hexdigest()


def hash_password_via_api(password: str, session: requests.Session) -> Optional[str]:
    """서버의 /test/hash-password 엔드포인트를 이용해 비밀번호를 해싱합니다."""
    try:
        response = session.post(
            HASH_PASSWORD_ENDPOINT,
            json={"password": password},
            timeout=10
        )
        if response.status_code == 200:
            data = response.json()
            return data.get("hashed_password")
        else:
            logger.warning(f"Hash password API failed: {response.status_code}")
            return None
    except Exception as e:
        logger.warning(f"Failed to hash password via API: {e}")
        return None


def generate_user_id(index: int) -> str:
    """테스트 계정의 고유한 user_id를 생성합니다."""
    return f"test_user_{index:04d}"


def generate_nickname(index: int) -> str:
    """테스트 계정의 nickname을 생성합니다."""
    return f"TestUser{index}"


async def create_account(
    session: aiohttp.ClientSession,
    user_id: str,
    nickname: str,
    hashed_password: str,
    profile_text: str,
    index: int
) -> Dict:
    """비동기로 하나의 계정을 생성합니다."""
    payload = {
        "user_id": user_id,
        "nickname": nickname,
        "password": hashed_password,
        "profile_bio": profile_text
    }
    
    try:
        async with session.post(
            REGISTER_ENDPOINT,
            json=payload,
            timeout=aiohttp.ClientTimeout(total=30)
        ) as response:
            result = await response.json()
            
            if response.status == 201:
                logger.info(f"[{index}] Account created: {user_id}")
                return {
                    "user_id": user_id,
                    "nickname": nickname,
                    "profile": profile_text[:100],  # 처음 100자
                    "status": "success",
                    "uuid": result.get("user_uuid"),
                    "timestamp": datetime.now().isoformat()
                }
            else:
                error_msg = f"Status {response.status}: {result}"
                logger.warning(f"[{index}] Account creation failed: {user_id} - {error_msg}")
                return {
                    "user_id": user_id,
                    "status": "failed",
                    "error": error_msg,
                    "timestamp": datetime.now().isoformat()
                }
    except asyncio.TimeoutError:
        error_msg = "Request timeout"
        logger.error(f"[{index}] Timeout creating account {user_id}")
        return {
            "user_id": user_id,
            "status": "timeout",
            "error": error_msg,
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        error_msg = str(e)
        logger.error(f"[{index}] Error creating account {user_id}: {error_msg}")
        return {
            "user_id": user_id,
            "status": "error",
            "error": error_msg,
            "timestamp": datetime.now().isoformat()
        }


async def create_accounts_batch(
    profiles: List[str],
    hashed_password: str,
    start_index: int,
    batch_size: int
) -> List[Dict]:
    """배치 단위로 여러 계정을 비동기로 생성합니다."""
    connector = aiohttp.TCPConnector(limit_per_host=10)
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = []
        
        for i, profile_text in enumerate(profiles[start_index:start_index+batch_size]):
            index = start_index + i + 1
            user_id = generate_user_id(index)
            nickname = generate_nickname(index)
            
            task = create_account(
                session,
                user_id,
                nickname,
                hashed_password,
                profile_text,
                index
            )
            tasks.append(task)
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        return results


def load_profiles() -> List[str]:
    """profile.txt에서 프로필 문구를 읽습니다."""
    try:
        with open(PROFILE_FILE, 'r', encoding='utf-8') as f:
            profiles = [line.strip() for line in f.readlines() if line.strip()]
        logger.info(f"Loaded {len(profiles)} profiles from {PROFILE_FILE}")
        return profiles
    except FileNotFoundError:
        logger.error(f"Profile file not found: {PROFILE_FILE}")
        return []


def save_results(results: List[Dict]):
    """결과를 JSON 파일로 저장합니다."""
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    logger.info(f"Results saved to {OUTPUT_FILE}")


def print_summary(results: List[Dict]):
    """생성 결과 요약을 출력합니다."""
    success_count = sum(1 for r in results if r.get("status") == "success")
    failed_count = sum(1 for r in results if r.get("status") == "failed")
    timeout_count = sum(1 for r in results if r.get("status") == "timeout")
    error_count = sum(1 for r in results if r.get("status") == "error")
    
    print("\n" + "="*60)
    print("계정 생성 결과 요약")
    print("="*60)
    print(f"총 요청: {len(results)}")
    print(f"✓ 성공: {success_count}")
    print(f"✗ 실패: {failed_count}")
    print(f"⏱ 타임아웃: {timeout_count}")
    print(f"⚠ 오류: {error_count}")
    print("="*60 + "\n")


async def main():
    """메인 함수: 1000개 계정 생성을 조율합니다."""
    logger.info("Starting account creation process")
    
    # 프로필 로드
    profiles = load_profiles()
    if not profiles:
        logger.error("No profiles to process")
        return
    
    # 최대 1000개만 처리
    profiles = profiles[:1000]
    total = len(profiles)
    
    # 비밀번호 해싱
    logger.info(f"Hashing password...")
    session = requests.Session()
    hashed_password = hash_password_via_api(DEFAULT_PASSWORD, session)
    
    if not hashed_password:
        # API 실패 시 클라이언트 측 해싱 사용 (SHA256)
        logger.warning("Using client-side SHA256 hashing")
        hashed_password = sha256_hash(DEFAULT_PASSWORD)
    
    logger.info(f"Password hash ready: {hashed_password[:20]}...")
    
    # 배치 단위로 계정 생성
    all_results = []
    
    for batch_start in range(0, total, BATCH_SIZE):
        batch_end = min(batch_start + BATCH_SIZE, total)
        batch_num = batch_start // BATCH_SIZE + 1
        total_batches = (total + BATCH_SIZE - 1) // BATCH_SIZE
        
        logger.info(f"Processing batch {batch_num}/{total_batches} ({batch_start+1}-{batch_end}/{total})")
        
        batch_results = await create_accounts_batch(
            profiles,
            hashed_password,
            batch_start,
            BATCH_SIZE
        )
        all_results.extend(batch_results)
        
        # 배치 간 지연 (서버 부하 완화)
        if batch_end < total:
            await asyncio.sleep(1)
    
    # 결과 저장 및 출력
    save_results(all_results)
    print_summary(all_results)
    
    logger.info("Account creation process completed")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Process interrupted by user")
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
