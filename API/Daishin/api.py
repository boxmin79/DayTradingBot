import win32com.client
import time
import ctypes

class DaishinAPI:
    def __init__(self):
        """대신증권 API 최상위 부모 클래스"""
        # COM객체 이름 정의
        try:
            # 1. 연결확인 객체 
            self.obj_CpCybos = win32com.client.Dispatch("CpUtil.CpCybos")
            # 2. 호출 제한 객체
            self.obj_CpLimit = win32com.client.Dispatch("CpUtil.CpLimit") 
            # 호출 제한 객체가 없으면 경고 출력
            if self.obj_CpLimit is None:
                print("!!! [오류] 대신증권 API 호출 제한 객체 생성 실패")
    
    
        
        except Exception as e:
            print(f"!!! [오류] 대신증권 API 객체 생성 실패: {e}")
        

        # 관리자 권한 확인
        if not ctypes.windll.shell32.IsUserAnAdmin():
            print("!!! [주의] 관리자 권한이 아닙니다.")
        

    def is_connected(self):
        """연결 상태 확인"""
        if self.CpCybos is None: return False
        return self.CpCybos.IsConnect == 1

    def wait_for_limit(self, limit_type=1):
         # limit_type 0: 주문 관련 1: 시세 요청 관련 2: 실시간 요청 관련 
        """
        [핵심] API 호출 제한 관리 메서드
        이 메서드가 정의되어 있어야 자식 클래스에서 에러가 발생하지 않습니다.
        """
        if self.obj_CpLimit is None:
            return

        remain_count = self.obj_CpLimit.GetLimitRemainCount(limit_type)
        if remain_count <= 0:
            while remain_count <= 0:
                remain_time = self.obj_CpLimit.LimitRequestRemainTime
                if remain_time > 0:
                    time.sleep(remain_time / 1000 + 0.1)
                else:
                    time.sleep(0.1)
                remain_count = self.obj_CpLimit.GetLimitRemainCount(limit_type)