import win32com.client
import time

class DaishinAPI:
    def __init__(self):
        # 공통 유틸리티 오브젝트 (연결 및 제한 확인용)
        self.obj_utils = win32com.client.Dispatch("CpUtil.CpCybos")

    def wait_for_limit(self, limit_type=1):
        """
        API 호출 제한을 '타이트'하게 관리하는 로직
        limit_type 1: 시세 조회 (제한: 15초당 60건)
        limit_type 2: 주문 요청
        """
        # 1. 1차 체크: 남은 횟수가 있으면 즉시 통과 (딜레이 0)
        remain_count = self.obj_utils.GetLimitRemainCount(limit_type)
        if remain_count > 0:
            return

        # 2. 제한에 걸린 경우: 풀릴 때까지 '무한 루프'로 대기
        print(f"    [Max] 제한 도달. 대기 모드 진입...", end='\r')
        
        while remain_count <= 0:
            # 서버가 알려주는 '해제까지 남은 시간' (단위: ms)
            remain_time = self.obj_utils.LimitRequestRemainTime
            
            # 남은 시간이 있다면 그만큼만 정확히 대기 (여유분 0.05초만 추가)
            if remain_time > 0:
                # 기존의 +1초 제거 -> 타이트하게 0.05초만 버퍼 둠
                time.sleep(remain_time / 1000 + 0.05)
            else:
                # 시간은 됐는데 횟수 갱신이 덜 된 경우 아주 잠깐 대기
                time.sleep(0.1)
            
            # 3. 재확인: 정말 제한이 풀렸는지 다시 체크
            remain_count = self.obj_utils.GetLimitRemainCount(limit_type)

        # 루프 탈출 = 제한 해제 완료
        # print("    [Res] 재개                    ", end='\r')

    def is_connected(self):
        """연결 상태 확인"""
        return self.obj_utils.IsConnect == 1