import os
import json
import asyncio
from google import genai
from google.genai import types
from dotenv import load_dotenv

# 환경 변수 로드 (.env 파일에서 GEMINI_KEY 로드)
load_dotenv()
GEMINI_API_KEY = os.getenv("GEMINI_KEY")
client = genai.Client(api_key=GEMINI_API_KEY)

models = ["gemini-3.1-flash-lite-preview", "gemini-3-flash-preview", "gemini-2.5-flash-lite", "gemini-2.5-flash"]

jobs = ["백엔드", "프론트엔드", "풀스택", "데이터 엔지니어", "임베디드", "AI 엔지니어", "정보보호/보안", "모바일 앱 개발", "데스크톱 게임 개발", "모바일 게임 개발",
    "시스템 프로그래밍", "전자모터/기계 제어", "전자회로 설계", "3D 모델러", "UI/UX 디자이너", "일러스트레이터", "품질관리 QA", "프로젝트 매니저 PM", "마케터", "기획자"] # 20 type
years = ["2-3년차", "5-7년차", "10년차 이상"] # 3 type
trends = ["최신 기술", "레거시 기술"] # 2 type

def get_batch_prompt(j, y, t):
    """1회 요청으로 10개의 프로필(인간 7, AI 3)을 생성하는 프롬프트"""
    return f"""
    당신은 하이브리드 팀 매칭 플랫폼 'Taskrit'에 등록될 작업자 프로필을 대량 생성하는 AI입니다.
    
    [배치 생성 조건]
    - 직군: {j}
    - 경력: {y}
    - 기술 스택 트렌드: {t}
    
    위 조건에 완벽하게 일치하는 다양한 프로필 데이터를 **총 10개** 생성해주세요. 
    반드시 7개는 인간("type": "human"), 3개는 AI 에이전트("type": "agent")로 구성해야 합니다.
    각 프로필의 내용은 서로 다르고 개성 있어야 합니다.
    
    [세부 작성 가이드]
    - 'abilityText': 이력서나 서비스 소개서에 들어갈 법한 자연스러운 한국어 텍스트(5~6문장 분량)
    - '{j}' 직군과 '{t}'에 어울리는 구체적인 기술 스택, 프레임워크, 도구 이름(예: Node.js, Kubernetes, C++, Figma 등)을 자연스럽게 문장에 녹이세요.
    - 시급/단가('cost'): 경력 '{y}'에 걸맞게 500~50000 사이의 정수로 다양하게 책정하세요.
    
    반드시 아래 JSON 배열(Array) 형식으로만 응답하세요. 마크다운 기호(```json) 없이 순수 JSON 배열만 출력해야 합니다.
    [
        {{
            "type": "human",
            "job_category": "{j}",
            "experience": "{y}",
            "tech_trend": "{t}",
            "abilityText": "안녕하세요, ...",
            "cost": 300
        }},
        ... (총 10개 객체)
    ]
    """

async def main():
    all_profiles = []
    
    # 20 * 3 * 2 = 120개의 조합 리스트 생성
    combinations = [(j, y, t) for j in jobs for y in years for t in trends]
    total_batches = len(combinations)
    
    # 15초씩 대기할 경우 예상 소요 시간 계산
    estimated_time_min = (total_batches * 16) / 60 
    print(f"총 {total_batches}개의 배치를 순차적으로 실행합니다. (총 1,200개 생성)")
    print(f"예상 소요 시간: 약 {estimated_time_min:.1f}분\n")
    
    for idx, (j, y, t) in enumerate(combinations):
        prompt = get_batch_prompt(j, y, t)
        print(f"[{idx+1}/{total_batches}] 생성 요청 | 직군: {j} | 경력: {y} | 트렌드: {t}")
        
        success = False
        for model_name in models:
            try:
                # API 호출
                response = client.models.generate_content(
                    model=model_name,
                    contents=prompt,
                    config=types.GenerateContentConfig(
                        temperature=0.7, # 10개가 서로 다르게 나오도록 약간의 무작위성 부여
                        response_mime_type="application/json",
                    )
                )
                
                # 응답 텍스트 정제 (마크다운 포맷 제거)
                cleaned_text = response.text.strip()
                if cleaned_text.startswith("```"):
                    cleaned_text = cleaned_text.split("\n", 1)[1] if "\n" in cleaned_text else cleaned_text[3:]
                    if cleaned_text.endswith("```"):
                        cleaned_text = cleaned_text[:-3]
                
                # JSON 파싱 및 저장
                batch_data = json.loads(cleaned_text.strip())
                
                if isinstance(batch_data, list):
                    all_profiles.extend(batch_data)
                    human_count = sum(1 for p in batch_data if p.get("type") == "human")
                    agent_count = sum(1 for p in batch_data if p.get("type") == "agent")
                    print(f"  └─ 성공: {len(batch_data)}개 파싱 완료 (모델: {model_name}, 인간 {human_count} / AI {agent_count})")

                    # 응답 실시간 출력
                    for i, profile in enumerate(batch_data):
                        print(f"    {i+1}. {profile['type']} - {profile['job_category']} ({profile['experience']}) | {profile['cost']}원")
                        print(f"    {i+1}. {profile['abilityText']}")
                        print("")
                    success = True
                    break # 성공 시 다음 모델 시도 중단
                else:
                    print(f"  └─ 오류: 응답이 배열 형태가 아닙니다. ({cleaned_text[:50]}...)")
                    
            except json.JSONDecodeError as e:
                print(f"  └─ 파싱 실패 (JSON 형식 오류, 모델: {model_name}): {e}")
            except Exception as e:
                print(f"  └─ API 호출 실패 (모델: {model_name}): {e}")
                
            # 실패 시 마지막 모델이 아니면 3초 대기
            if model_name != models[-1]:
                print(f"  └─ {model_name} 실패. 3초 대기 후 다음 모델로 재시도합니다...")
                await asyncio.sleep(3)
                
        if not success:
            print("  └─ 모든 모델 배포 실패. 다음 요청으로 넘어갑니다.")
            
        # 마지막 배치가 아니면 API 한도 초과(429)를 막기 위해 대기
        if idx < total_batches - 1:
            print("  └─ 분당 제한 준수를 위해 15초 대기 중...\n")
            await asyncio.sleep(15)
            
    # 전체 완료 후 결과 파일 저장
    output_filename = "dummy_profiles_batch.json"
    with open(output_filename, "w", encoding="utf-8") as f:
        json.dump(all_profiles, f, ensure_ascii=False, indent=2)
        
    print(f"\n✅ 모든 생성 작업 완료! 총 {len(all_profiles)}개의 데이터가 '{output_filename}'에 저장되었습니다.")

if __name__ == "__main__":
    asyncio.run(main())