from PyQt5.QAxContainer import QAxWidget
from PyQt5.QtCore import QEventLoop

class KiwoomAPI:
    def __init__(self):
        """
        키움 API 클래스 초기화
        - OCX 컨트롤 생성
        - 로그인 이벤트 슬롯 연결
        """
        # 1. 키움증권 OpenAPI+ ActiveX 컨트롤 로드
        self.ocx = QAxWidget("KHOPENAPI.KHOpenAPICtrl.1")
        
        # 2. 비동기 처리를 위한 이벤트 루프 변수
        self.login_loop = None
        
        # 3. 로그인 결과 이벤트 연결 (서버 응답 시 _handler_login 실행)
        self.ocx.OnEventConnect.connect(self._handler_login)

    def is_connected(self):
        """
        현재 서버 접속 상태 확인
        Return:
            True: 연결됨
            False: 연결 안 됨
        """
        # GetConnectState: 0(미연결), 1(연결완료)
        state = self.ocx.dynamicCall("GetConnectState()")
        return state == 1

    def start_login(self):
        """
        로그인 창을 띄우고 접속이 완료될 때까지 대기 (Blocking)
        """
        if self.is_connected():
            return True
        
        print("[System] 키움증권 서버에 로그인을 시도합니다...")
        
        # 로그인 요청
        self.ocx.dynamicCall("CommConnect()")
        
        # 이벤트 루프 생성 및 대기 (응답이 올 때까지 코드 실행 멈춤)
        self.login_loop = QEventLoop()
        self.login_loop.exec_()
        
        return self.is_connected()

    def _handler_login(self, err_code):
        """
        [이벤트 슬롯] 로그인 결과 수신
        err_code: 0(성공), 그 외(실패)
        """
        if err_code == 0:
            print("[System] 로그인 성공: 서버에 정상적으로 연결되었습니다.")
        else:
            print(f"[System] 로그인 실패: 에러코드 {err_code}")
            print("  (팁: 32비트 환경 및 관리자 권한을 확인하세요)")
        
        # 대기 중이던 이벤트 루프 종료 -> start_login()의 대기 해제
        if self.login_loop and self.login_loop.isRunning():
            self.login_loop.exit()

    def ensure_connected(self):
        """
        [접속 보장 메서드]
        - 연결되어 있으면 즉시 True 반환
        - 연결 안 되어 있으면 로그인 시도 후 결과 반환
        """
        if self.is_connected():
            return True
        
        print("[Warn] 서버 연결이 확인되지 않습니다. 연결을 시도합니다.")
        return self.start_login()

    # --- [공통 메서드] 자식 클래스들이 사용할 기본 통신 함수 ---
    
    def set_input_value(self, id, value):
        """TR 입력값 설정"""
        self.ocx.dynamicCall("SetInputValue(QString, QString)", id, value)

    def comm_rq_data(self, rqname, trcode, next, screen_no):
        """TR 데이터 요청"""
        return self.ocx.dynamicCall("CommRqData(QString, QString, int, QString)", rqname, trcode, next, screen_no)

    def disconnect_real_data(self, screen_no):
        """화면 번호별 실시간 데이터 연결 해제"""
        self.ocx.dynamicCall("DisconnectRealData(QString)", screen_no)