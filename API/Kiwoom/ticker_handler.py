import os
import pandas as pd
from datetime import datetime
from API.Kiwoom.api import KiwoomAPI

class TickerHandler(KiwoomAPI):
    def __init__(self):
        super().__init__()
        self.save_dir = os.path.abspath(os.path.join(os.getcwd(), "data", "ticker"))
        self.filtered_path = os.path.join(self.save_dir, "filtered_tickers.csv")
        self.raw_path = os.path.join(self.save_dir, "tickers.csv")
        self.log_path = os.path.join(self.save_dir, "last_update.txt")

    def should_update(self):
        """오늘 이미 업데이트를 했는지, 그리고 필요한 파일들이 존재하는지 확인합니다."""
        # 1. 필수 CSV 파일 존재 여부 확인
        files_exist = os.path.exists(self.raw_path) and os.path.exists(self.filtered_path)
        if not files_exist:
            print("--- [알림] 종목 리스트 파일이 없습니다. 수집을 시작합니다. ---")
            return True

        # 2. 로그 파일 존재 여부 확인
        if not os.path.exists(self.log_path):
            print("--- [알림] 업데이트 로그가 없습니다. 수집을 시작합니다. ---")
            return True

        # 3. 날짜 비교
        with open(self.log_path, "r", encoding="utf-8") as f:
            log_content = f.read().strip()
            if not log_content:
                return True
                
            last_date = log_content.split(' ')[0] # '2026-02-07' 부분 추출
            today = datetime.now().strftime('%Y-%m-%d')
            
            if last_date == today:
                print(f"--- [알림] 오늘({today}) 이미 업데이트를 완료했으며, 파일이 모두 존재합니다. ---")
                return False
            
            print(f"--- [알림] 마지막 업데이트({last_date}) 이후 날짜가 변경되었습니다. ---")
            return True

    def collect_and_save(self):
        if not self.is_connected():
            print("!!! [중단] 서버 연결이 없습니다.")
            return

        # 업데이트 필요 여부 체크 (파일 존재 여부 및 날짜 확인)
        if not self.should_update():
            return

        if not os.path.exists(self.save_dir):
            os.makedirs(self.save_dir)

        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print(f"--- [데이터 수집] 새로운 종목 리스트 수집 시작 ({current_time}) ---")
        
        # 1. 기초 리스트 및 제외 그룹 확보
        kospi = self.ocx.dynamicCall("GetCodeListByMarket(QString)", "0").split(';')[:-1]
        kosdaq = self.ocx.dynamicCall("GetCodeListByMarket(QString)", "10").split(';')[:-1]
        etf_list = self.ocx.dynamicCall("GetCodeListByMarket(QString)", "8").split(';')[:-1]
        etn_list = self.ocx.dynamicCall("GetCodeListByMarket(QString)", "5").split(';')[:-1]
        exclude_set = set(etf_list + etn_list)
        
        all_tickers = [("KOSPI", kospi), ("KOSDAQ", kosdaq)]
        raw_list = []
        filtered_list = []

        for market_name, tickers in all_tickers:
            for code in tickers:
                name = str(self.ocx.dynamicCall("GetMasterCodeName(QString)", [code])).strip()
                stock_state = str(self.ocx.dynamicCall("GetMasterStockState(QString)", [code])).strip()
                construction = str(self.ocx.dynamicCall("GetMasterConstruction(QString)", [code])).strip()
                prev_price_raw = self.ocx.dynamicCall("GetMasterLastPrice(QString)", [code])
                prev_price = int(prev_price_raw) if str(prev_price_raw).isdigit() else 0
                stock_cnt_raw = self.ocx.dynamicCall("GetMasterListedStockCnt(QString)", [code])
                stock_cnt = int(stock_cnt_raw) if str(stock_cnt_raw).isdigit() else 0
                reg_day = self.ocx.dynamicCall("GetMasterListedStockDate(QString)", [code])

                # 시가총액 계산
                market_cap = prev_price * stock_cnt

                row = {
                    'code': code,
                    'name': name,
                    'market': market_name,
                    'stock_count': stock_cnt,
                    'market_cap': market_cap,
                    'reg_day': str(reg_day).strip(),
                    'prev_price': prev_price,
                    'construction': construction,
                    'state': stock_state
                }

                raw_list.append(row)

                # 필터링 조건
                if code in exclude_set: continue
                if not code.endswith('0'): continue
                if code.startswith('9'): continue
                if "스팩" in name: continue
                if prev_price < 1000: continue
                if market_cap < 50000000000: continue
                if any(kw in stock_state or kw in construction for kw in ["관리", "정리", "거래정지"]):
                    continue

                filtered_list.append(row)

        # 2. 파일 저장
        try:
            pd.DataFrame(raw_list).to_csv(self.raw_path, index=False, encoding='utf-8-sig')
            pd.DataFrame(filtered_list).to_csv(self.filtered_path, index=False, encoding='utf-8-sig')
            
            with open(self.log_path, "w", encoding="utf-8") as f:
                f.write(current_time)
            
            print(f"--- [저장 완료] ---")
            print(f"원본: {len(raw_list)}건 / 필터: {len(filtered_list)}건 저장되었습니다.")
            
        except Exception as e:
            print(f"!!! [저장 실패] {e}")