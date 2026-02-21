import os
import requests
import pandas as pd
from dotenv import load_dotenv

# 1. 환경 변수 로드 (경로 정밀 탐색)
current_dir = os.path.dirname(os.path.abspath(__file__))
env_path = os.path.join(current_dir, "..", "..", ".env")
load_dotenv(env_path)

CLIENT_ID = os.getenv("KIWOOM_CLIENT_ID", "").strip().replace('"', '').replace("'", "")
CLIENT_SECRET = os.getenv("KIWOOM_CLIENT_SECRET", "").strip().replace('"', '').replace("'", "")
MODE = os.getenv("KIWOOM_MODE", "MOCK")

# 도메인 설정 (키와 도메인이 맞아야 8001 에러가 안 납니다)
if MODE == "REAL":
    DOMAIN = "api.kiwoom.com"
else:
    DOMAIN = "mockapi.kiwoom.com"

BASE_URL = f"https://{DOMAIN}"

def diagnose_and_get_token():
    print(f"--- 🔍 인증 환경 진단 ---")
    print(f"접속 도메인: {BASE_URL}")
    print(f"현재 모드: {MODE} (실전투자 키라면 REAL이어야 함)")
    print(f"ID 확인: {CLIENT_ID[:4]}*** (길이: {len(CLIENT_ID)})")
    print(f"------------------------")

    url = f"{BASE_URL}/oauth2/token"
    headers = {"Content-Type": "application/json;charset=UTF-8"}
    data = {
        "grant_type": "client_credentials",
        "appkey": CLIENT_ID,
        "secretkey": CLIENT_SECRET
    }
    
    try:
        res = requests.post(url, headers=headers, json=data)
        result = res.json()
        
        # 키움 API는 인증 실패 시에도 HTTP 200을 줄 수 있으므로 내부 코드를 확인해야 함
        if "access_token" in result:
            print("✅ 인증 성공! 토큰이 발급되었습니다.")
            return result["access_token"]
        else:
            print(f"❌ 인증 실패")
            print(f"서버 메시지: {result.get('return_msg')}")
            print(f"팁: 키가 {MODE}용이 맞는지 홈페이지에서 재확인하세요.")
            return None
    except Exception as e:
        print(f"❗ 요청 중 오류 발생: {e}")
        return None

if __name__ == "__main__":
    token = diagnose_and_get_token()
    if token:
        # 이후 종목 정보 수집 로직...
        print("데이터 수집을 계속할 수 있습니다.")