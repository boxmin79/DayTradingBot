import os
import sys
import pandas as pd
from datetime import datetime
from PyQt5.QtWidgets import QApplication

# 프로젝트 루트 경로 추가 (API 패키지 인식을 위해)
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from API.Kiwoom.api import KiwoomAPI

class TickerHandler(KiwoomAPI):
    def __init__(self):
        super().__init__()
        # 경로 설정: 날짜를 제거한 고정 파일명 사용
        base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        self.save_dir = os.path.join(base_dir, "data", "ticker")
        self.raw_path = os.path.join(self.save_dir, "tickers.parquet")
        self.filtered_path = os.path.join(self.save_dir, "filtered_tickers.parquet")
        self.log_path = os.path.join(self.save_dir, "last_update.txt") # 업데이트 날짜 저장용 txt

    def should_update(self):
        """오늘 이미 업데이트를 했는지 확인합니다."""
        # 1. 필수 파일들이 없으면 업데이트 필요
        if not (os.path.exists(self.raw_path) and os.path.exists(self.filtered_path)):
            return True

        # 2. 로그 파일이 없으면 업데이트 필요
        if not os.path.exists(self.log_path):
            return True

        # 3. 마지막 업데이트 날짜와 오늘 날짜 비교
        with open(self.log_path, "r", encoding="utf-8") as f:
            log_content = f.read().strip()
            if not log_content:
                return True
                
            last_date = log_content.split(' ')[0] # '2026-02-07' 형식 추출
            today = datetime.now().strftime('%Y-%m-%d')
            
            if last_date == today:
                print(f"--- [알림] 오늘({today}) 이미 종목 리스트 업데이트를 완료했습니다. ---")
                return False
            
            return True

    def collect_and_save(self):
        if not self.ensure_connected():
            print("!!! [중단] 서버 연결이 없습니다.")
            return

        if not self.should_update():
            return

        if not os.path.exists(self.save_dir):
            os.makedirs(self.save_dir)

        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print(f"--- [데이터 수집] 종목 리스트 및 상장일 수집 시작 ({current_time}) ---")
        
        kospi = self.ocx.dynamicCall("GetCodeListByMarket(QString)", "0").split(';')[:-1]
        kosdaq = self.ocx.dynamicCall("GetCodeListByMarket(QString)", "10").split(';')[:-1]
        
        exclude_set = set(self.ocx.dynamicCall("GetCodeListByMarket(QString)", "8").split(';')[:-1] + 
                          self.ocx.dynamicCall("GetCodeListByMarket(QString)", "5").split(';')[:-1])
        
        all_tickers = [("KOSPI", kospi), ("KOSDAQ", kosdaq)]
        raw_list = []
        filtered_list = []

        for market_name, tickers in all_tickers:
            for code in tickers:
                name = str(self.ocx.dynamicCall("GetMasterCodeName(QString)", code)).strip()
                state = str(self.ocx.dynamicCall("GetMasterStockState(QString)", code)).strip()
                construction = str(self.ocx.dynamicCall("GetMasterConstruction(QString)", code)).strip()
                price = int(self.ocx.dynamicCall("GetMasterLastPrice(QString)", code) or 0)
                stock_cnt = int(self.ocx.dynamicCall("GetMasterListedStockCnt(QString)", code) or 0)
                market_cap = price * stock_cnt
                
                # --- [추가된 로직] 상장일 수집 ---
                # GetMasterListedStockDate "YYYY/MM/DD" 또는 "YYYYMMDD" 형태의 문자열을 반환합니다.
                listing_date = str(self.ocx.dynamicCall("GetMasterListedStockDate(QString)", code)).strip()
                row = {
                    'code': code,
                    'name': name,
                    'market': market_name,
                    'market_cap': market_cap,
                    'prev_price': price,
                    'state': state,
                    'construction': construction,
                    'listing_date': listing_date  # 결과 딕셔너리에 추가
                }
                raw_list.append(row)

                if code in exclude_set: continue
                if not code.endswith('0'): continue
                if price < 1000 or market_cap < 50000000000: continue
                if any(kw in state or kw in construction for kw in ["관리", "정리", "거래정지"]):
                    continue

                filtered_list.append(row)

        try:
            pd.DataFrame(raw_list).to_parquet(self.raw_path, index=False)
            pd.DataFrame(filtered_list).to_parquet(self.filtered_path, index=False)
            
            with open(self.log_path, "w", encoding="utf-8") as f:
                f.write(current_time)
            
            print(f"--- [완료] 상장일 포함 데이터 저장: 원본 {len(raw_list)}건 / 필터 {len(filtered_list)}건 ---")
            
        except Exception as e:
            print(f"!!! [저장 실패] {e}")
            
#테스트 코드
if __name__ == "__main__":
    app = QApplication(sys.argv)
    handler = TickerHandler()
    handler.collect_and_save()