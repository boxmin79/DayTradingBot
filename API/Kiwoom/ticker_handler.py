import os
import pandas as pd
from datetime import datetime
from API.Kiwoom.api import KiwoomAPI

class TickerHandler(KiwoomAPI):
    def __init__(self):
        super().__init__()
        # 경로 설정: 날짜를 제거한 고정 파일명 사용
        self.save_dir = os.path.abspath(os.path.join(os.getcwd(), "data", "ticker"))
        self.raw_path = os.path.join(self.save_dir, "tickers.csv")
        self.filtered_path = os.path.join(self.save_dir, "filtered_tickers.csv")
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

        # 업데이트가 필요한 경우에만 진행
        if not self.should_update():
            return

        if not os.path.exists(self.save_dir):
            os.makedirs(self.save_dir)

        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print(f"--- [데이터 수집] 종목 리스트 수집 시작 ({current_time}) ---")
        
        # 시장별 종목 리스트 확보
        kospi = self.ocx.dynamicCall("GetCodeListByMarket(QString)", "0").split(';')[:-1]
        kosdaq = self.ocx.dynamicCall("GetCodeListByMarket(QString)", "10").split(';')[:-1]
        
        # 제외 종목(ETF/ETN) 확보
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

                row = {
                    'code': code,
                    'name': name,
                    'market': market_name,
                    'market_cap': market_cap,
                    'prev_price': price,
                    'state': state,
                    'construction': construction
                }
                raw_list.append(row)

                # 필터링 조건 적용
                if code in exclude_set: continue
                if not code.endswith('0'): continue # 우선주 제외
                if price < 1000 or market_cap < 50000000000: continue # 동전주/소형주 제외
                if any(kw in state or kw in construction for kw in ["관리", "정리", "거래정지"]):
                    continue

                filtered_list.append(row)

        # 파일 저장 (고정 파일명)
        try:
            pd.DataFrame(raw_list).to_csv(self.raw_path, index=False, encoding='utf-8-sig')
            pd.DataFrame(filtered_list).to_csv(self.filtered_path, index=False, encoding='utf-8-sig')
            
            # 마지막 업데이트 날짜 기록
            with open(self.log_path, "w", encoding="utf-8") as f:
                f.write(current_time)
            
            print(f"--- [완료] 원본: {len(raw_list)}건 / 필터: {len(filtered_list)}건 저장되었습니다. ---")
            
        except Exception as e:
            print(f"!!! [저장 실패] {e}")