import win32com.client
import time
import ctypes

class DaishinAPI:
    def __init__(self):
        """대신증권 API 최상위 부모 클래스"""
        self.obj_utils = None
        self.obj_trade = None

        # 관리자 권한 확인
        if not ctypes.windll.shell32.IsUserAnAdmin():
            print("!!! [주의] 관리자 권한이 아닙니다.")

        try:
            # COM 객체 생성
            self.obj_utils = win32com.client.Dispatch("CpUtil.CpCybos")
            self.obj_trade = win32com.client.Dispatch("CpTrade.CpTdUtil")
        except Exception as e:
            print(f"!!! [오류] 대신증권 API 객체 생성 실패: {e}")

    def is_connected(self):
        """연결 상태 확인"""
        if self.obj_utils is None: return False
        return self.obj_utils.IsConnect == 1

    def wait_for_limit(self, limit_type=1):
        """
        [핵심] API 호출 제한 관리 메서드
        이 메서드가 정의되어 있어야 자식 클래스에서 에러가 발생하지 않습니다.
        """
        if self.obj_utils is None:
            return

        remain_count = self.obj_utils.GetLimitRemainCount(limit_type)
        if remain_count <= 0:
            while remain_count <= 0:
                remain_time = self.obj_utils.LimitRequestRemainTime
                if remain_time > 0:
                    time.sleep(remain_time / 1000 + 0.1)
                else:
                    time.sleep(0.1)
                remain_count = self.obj_utils.GetLimitRemainCount(limit_type)