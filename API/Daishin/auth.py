import os
import time
from pywinauto import Application
import ctypes

def login_daishin(id, pwd, pwd_cert):
    """
    대신증권 Cybos Plus 자동 로그인 실행
    id: 사용자 ID
    pwd: 로그인 비밀번호
    pwd_cert: 공인인증서 비밀번호
    """
    
    # 관리자 권한 확인 (대신 API는 관리자 권한이 필수입니다)
    if not ctypes.windll.shell32.IsUserAnAdmin():
        print("!!! [경고] 관리자 권한으로 실행해 주세요.")
        return False

    print("--- [대신증권] 자동 로그인 시도 중 ---")
    
    # 기존에 실행 중인 프로세스가 있다면 종료 (깔끔한 재로그인을 위해)
    os.system('taskkill /IM dibscenter.exe /F')
    os.system('taskkill /IM cpstart.exe /F')
    os.system('taskkill /IM coStarter.exe /F')
    time.sleep(2)

    # Cybos Plus 실행 경로 (보통 C:\daishin\cybosplus\cpstart.exe)
    path = r"C:\daishin\cybosplus\cpstart.exe"
    
    # 프로세스 실행 및 로그인 정보 전달
    app = Application().start(f'{path} /id:{id} /pwd:{pwd} /pwdcert:{pwd_cert} /autologin')
    
    print(">>> 로그인이 진행 중입니다. 약 20~30초 소요됩니다...")
    
    # 로그인 완료 여부 체크 루프
    import win32com.client
    obj_cpstatus = win32com.client.Dispatch("CpUtil.CpCybos")
    
    timeout = 60  # 최대 60초 대기
    start_time = time.time()
    
    while obj_cpstatus.IsConnect == 0:
        if time.time() - start_time > timeout:
            print("!!! [실패] 로그인 시간 초과")
            return False
        time.sleep(1)
        
    print(f"--- [성공] 대신증권 연결 완료 (시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}) ---")
    return True

if __name__ == "__main__":
    # 테스트 실행 (본인의 정보를 입력하세요)
    # login_daishin('아이디', '비밀번호', '인증서비밀번호')
    pass