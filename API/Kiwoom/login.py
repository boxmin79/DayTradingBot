import sys
from PyQt5.QtWidgets import QApplication
# 동일 폴더 내의 api.py에서 KiwoomAPI 클래스를 가져옵니다.
from API.Kiwoom.api import KiwoomAPI

class KiwoomLogin(KiwoomAPI):
    def __init__(self):
        super().__init__()
        print("[System] KiwoomLogin 인스턴스 가동")

    def start_login(self):
        print("[System] 로그인 창을 실행합니다...")
        self.comm_connect()  # 부모 클래스(api.py)의 메서드 호출
        self.show_account_info()

    def show_account_info(self):
        """접속 성공 후 계정 정보 출력"""
        user_id = self.ocx.dynamicCall("GetLoginInfo(QString)", "USER_ID")
        account_count = self.ocx.dynamicCall("GetLoginInfo(QString)", "ACCOUNT_CNT")
        account_list = self.ocx.dynamicCall("GetLoginInfo(QString)", "ACCNO")
        
        accounts = account_list.split(';')[:-1]
        
        print("=" * 40)
        print(f"  사용자 ID : {user_id}")
        print(f"  보유 계정수: {account_count}")
        print(f"  계정 목록  : {accounts}")
        print("=" * 40)
        print("[System] 연결 확인 완료!")

if __name__ == "__main__":
    app = QApplication(sys.argv)
    
    # 클래스 이름을 파일명과 맞춰 KiwoomLogin으로 유지하거나 
    # 더 줄이고 싶다면 Login으로 바꿔도 무방합니다.
    manager = KiwoomLogin()
    manager.start_login()
    
    sys.exit(app.exec_())