from PyQt5.QAxContainer import QAxWidget
from PyQt5.QtCore import QEventLoop
import sys

class KiwoomAPI:
    def __init__(self):
        """
        키움 API 클래스 초기화
        - OCX 컨트롤 생성 및 로그인 이벤트 연결
        """
        # 1. 키움증권 OpenAPI+ ActiveX 컨트롤 로드
        self.ocx = QAxWidget("KHOPENAPI.KHOpenAPICtrl.1")
        
        # 2. 비동기 처리를 위한 이벤트 루프 변수
        self.login_loop = None
        
        # 3. 로그인 결과 이벤트 연결
        self.ocx.OnEventConnect.connect(self._handler_login)

    def is_connected(self):
        """현재 서버 접속 상태 확인 (0: 미연결, 1: 연결완료)"""
        state = self.ocx.dynamicCall("GetConnectState()")
        return state == 1

    def comm_connect(self):
        """
        로그인 창을 실행합니다. (login.py와의 호환성을 위해 명칭 유지)
        """
        if self.is_connected():
            print("[System] 이미 서버에 연결되어 있습니다.")
            return True
        
        print("[System] 키움증권 로그인을 시도합니다...")
        self.ocx.dynamicCall("CommConnect()")
        
        self.login_loop = QEventLoop()
        self.login_loop.exec_()
        return self.is_connected()

    def start_login(self):
        """comm_connect의 별칭 메서드"""
        return self.comm_connect()

    def _handler_login(self, err_code):
        """로그인 결과 수신 이벤트 슬롯"""
        if err_code == 0:
            print("[System] 로그인 성공: 서버에 정상 연결되었습니다.")
        else:
            print(f"[System] 로그인 실패: 에러코드 {err_code}")
        
        if self.login_loop and self.login_loop.isRunning():
            self.login_loop.exit()

    def ensure_connected(self):
        """접속 상태를 확인하고 연결되지 않았으면 로그인을 시도합니다."""
        if self.is_connected():
            return True
        return self.comm_connect()

    # --- TR 통신을 위한 공통 메서드 ---
    def set_input_value(self, id, value):
        """TR 입력값 설정"""
        self.ocx.dynamicCall("SetInputValue(QString, QString)", id, value)

    def comm_rq_data(self, rqname, trcode, next, screen_no):
        """TR 데이터 요청"""
        return self.ocx.dynamicCall("CommRqData(QString, QString, int, QString)", rqname, trcode, next, screen_no)

    def disconnect_real_data(self, screen_no):
        """화면 번호별 실시간 데이터 연결 해제"""
        self.ocx.dynamicCall("DisconnectRealData(QString)", screen_no)