import os
import time
import win32com.client
# from pywinauto import application
from dotenv import load_dotenv

# .env 파일 로드
load_dotenv()

class DaishinLogin:
    def __init__(self):
        # .env에서 계정 정보 로드
        self.id = os.getenv("DAISHIN_ID")
        self.pwd = os.getenv("DAISHIN_PWD")
        self.pwd_cert = os.getenv("DAISHIN_PWD_CERT")
        self.obj_utils = win32com.client.Dispatch("CpUtil.CpCybos")

    def run_login(self):
        # 1. 이미 연결되어 있다면 종료
        if self.obj_utils.IsConnect == 1:
            print("--- [알림] 이미 대신증권 서버에 연결되어 있습니다. ---")
            return True

        # 2. 기존 프로세스 정리
        print("--- [초기화] 기존 프로세스 종료 중 ---")
        os.system("taskkill /im DibsCenter.exe /f 2>nul")
        os.system("taskkill /im CpStart.exe /f 2>nul")
        time.sleep(2)

        # 3. 기존 경로에 새로운 실행 인자 적용
        # 우리가 사용하던 CpStart.exe 경로
        app_path = r'C:\DAISHIN\CYBOSPLUS\CpStart.exe'
        
        if not os.path.exists(app_path):
            print(f"!!! [오류] 경로를 찾을 수 없습니다: {app_path}")
            return False

        print(f"--- [실행] 대신증권 로그인 시도 (ID: {self.id}) ---")
        
        # 사용자님이 주신 /prj:cp 및 /autostart 인자 적용
        # /prj:cp -> Cybos Plus 프로젝트 모드 실행
        # /autostart -> 로그인 후 자동 시작
        cmd = f'{app_path} /prj:cp /id:{self.id} /pwd:{self.pwd} /pwdcert:{self.pwd_cert} /autostart'
        
        try:
            app = application.Application()
            app.start(cmd)
            
            # 로그인 창이 처리되는 동안 대기
            print(">>> 서버 접속 및 인증 처리 중... (약 20초 소요)")
            time.sleep(20)
            
        except Exception as e:
            print(f"!!! [실행 실패] {e}")
            return False

        # 4. 최종 연결 및 계좌 세션 확인
        if self.obj_utils.IsConnect == 1:
            print(f"--- [성공] 대신증권 연결 완료! (시각: {time.strftime('%H:%M:%S')}) ---")
            
            # 12977 에러 방지를 위해 계좌 세션 한 번 더 건드려주기
            win32com.client.Dispatch("CpTrade.CpTdUtil").TradeInit(0)
            return True
        else:
            print("!!! [실패] PLUS가 정상적으로 연결되지 않았습니다. 로그인 창의 상태를 확인하세요.")
            return False

if __name__ == "__main__":
    login_module = DaishinLogin()
    login_module.run_login()