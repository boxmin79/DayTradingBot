import requests
import os
from datetime import datetime
from dotenv import load_dotenv

class KiwoomRestBase:
    """
    [키움 REST API 문서.xlsx] 규격을 엄격히 준수하는 최상위 클래스
    """
    def __init__(self):
        load_dotenv()
        
        # 문서 규격: appkey, secretkey 명칭 사용
        self.appkey = os.getenv("KIWOOM_CLIENT_ID")
        self.secretkey = os.getenv("KIWOOM_CLIENT_SECRET")
        
        # 모드 설정
        mode_env = os.getenv("KIWOOM_MODE", "MOCK").upper()
        self.mode = mode_env if mode_env in ["REAL", "MOCK"] else "MOCK"
        
        # 문서 기반 도메인 설정
        if self.mode == "REAL":
            self.base_url = "https://api.kiwoom.com"
        else:
            self.base_url = "https://mockapi.kiwoom.com"
            
        print(f"📡 [SYSTEM] 키움 REST API {self.mode} 서버 접속 준비")
        self.access_token = None

    def _get_access_token(self):
        """
        문서 규격: POST /oauth2/token
        Body: grant_type, appkey, secretkey
        """
        url = f"{self.base_url}/oauth2/token"
        headers = {"Content-Type": "application/json;charset=UTF-8"}
        
        # 문서에 명시된 Body 파라미터 명칭 적용
        payload = {
            "grant_type": "client_credentials",
            "appkey": self.appkey,
            "secretkey": self.secretkey
        }
        
        try:
            response = requests.post(url, headers=headers, json=payload)
            if response.status_code == 200:
                data = response.json()
                # 문서 규격: 응답에서 access_token 추출
                self.access_token = data.get("access_token")
                print(f"✅ [{self.mode}] 접근토큰 발급 성공")
                return self.access_token
            else:
                print(f"❌ 토큰 발급 실패: {response.status_code} - {response.text}")
                return None
        except Exception as e:
            print(f"❌ 인증 중 예외 발생: {e}")
            return None

    def _send_request(self, method, endpoint, api_id, params=None, data=None):
        """
        문서 규격: Header에 api-id(TR명)와 Authorization(Bearer) 필수 포함
        """
        if not self.access_token:
            if not self._get_access_token():
                return None

        url = f"{self.base_url}{endpoint}"
        headers = {
            "Content-Type": "application/json;charset=UTF-8",
            "authorization": f"Bearer {self.access_token}",
            "api-id": api_id  # 문서에서 요구하는 TR ID
        }

        try:
            if method.upper() == "GET":
                response = requests.get(url, headers=headers, params=params)
            else:
                response = requests.post(url, headers=headers, json=data)
            
            return self._handle_response(response)
        except Exception as e:
            print(f"❌ API 통신 오류: {e}")
            return None

    def _handle_response(self, response):
        if response.status_code == 200:
            return response.json()
        else:
            print(f"❌ API 에러 [{response.status_code}]: {response.text}")
            return None

    def save_to_csv(self, df, folder_name, file_prefix):
        save_path = f"data/{folder_name}"
        if not os.path.exists(save_path): os.makedirs(save_path)
        file_name = f"{file_prefix}_{self.mode}_{datetime.now().strftime('%Y%m%d')}.csv"
        full_path = os.path.join(save_path, file_name)
        df.to_csv(full_path, index=False, encoding='utf-8-sig')
        print(f"💾 데이터 저장 완료: {full_path}")