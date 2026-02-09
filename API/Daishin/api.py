import win32com.client
import time
import ctypes
import sys

class DaishinAPI:
    def __init__(self, limit_type):
        """대신증권 API 최상위 부모 클래스"""
        self.obj_CpCybos = None
        self.limit_type = limit_type        
        # 32비트 파이썬 환경 확인 (대신증권 API는 32비트 전용)
        if sys.maxsize > 2**32:
            print("!!! [치명적 오류] 64비트 파이썬이 감지되었습니다. 대신증권 API는 32비트(x86) 파이썬 환경에서만 동작합니다.")

        # COM객체 이름 정의
        try:
            # 연결확인 객체 
            self.obj_CpCybos = win32com.client.Dispatch("CpUtil.CpCybos")
         
            # 연결 확인 객체가 없으면 경고 출력
            if self.obj_CpCybos is None:
                print("!!! [오류] 대신증권 API 연결 확인 객체 생성 실패")
    
        
        except Exception as e:
            print(f"!!! [오류] 대신증권 API 객체 생성 실패: {e}")
            print("    -> Cybos Plus가 설치되어 있는지, 또는 32비트 파이썬 환경인지 확인하세요.")
        

        # 관리자 권한 확인
        if not ctypes.windll.shell32.IsUserAnAdmin():
            print("!!! [주의] 관리자 권한이 아닙니다.")
        

    def is_connected(self):
        """연결 상태 확인"""
        if self.obj_CpCybos is None: return False
        return self.obj_CpCybos.IsConnect == 1

    def wait_for_limit(self):
        """호출 제한 대기"""
        if self.obj_CpCybos is None:
            print("!!! [오류] 대신증권 API 연결 객체가 없습니다.")
            return

        remain_count = self.obj_CpCybos.GetLimitRemainCount(self.limit_type)
        if remain_count <= 0:
            print(f"API 호출 제한 도달! 대기 중... (limit_type={self.limit_type})")
            while remain_count <= 0:
                remain_time = self.obj_CpCybos.LimitRequestRemainTime
                if remain_time > 0:
                    time.sleep(remain_time / 1000 + 0.1)
                else:
                    time.sleep(0.1)
                remain_count = self.obj_CpCybos.GetLimitRemainCount(self.limit_type)
            

# 테스트 코드
if __name__ == "__main__":
    api = DaishinAPI(limit_type=1)
    if api.is_connected():
        print("대신증권 API에 정상적으로 연결되었습니다.")
    else:
        print("대신증권 API에 연결되지 않았습니다.")
    api.wait_for_limit()
    # 호출 제한 대기 테스트
    print("호출 제한 대기 완료.")
    print("남은 호출 횟수:", api.obj_CpCybos.GetLimitRemainCount(api.limit_type))