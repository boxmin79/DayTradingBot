import os
import sys
import pandas as pd
from datetime import datetime, timedelta
import holidays  # conda install holidays

# 프로젝트 루트 경로 추가
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(BASE_DIR)

from API.Daishin.stock_chart import CpStockChart

class MinuteChartUpdater:
    def __init__(self):
        self.api = CpStockChart()
        self.save_dir = os.path.join(BASE_DIR, "data", "chart", "minute")
        os.makedirs(self.save_dir, exist_ok=True)
        # 한국 공휴일 정보 로드
        self.kr_holidays = holidays.KR()

    def check_connection(self):
        """대신증권 API 연결 상태 확인"""
        if not self.api.is_connected():
            print("!!! [오류] 대신증권 API(Cybos Plus)가 연결되어 있지 않습니다.")
            return False
        return True

    def get_updated_data(self, ticker):
        """
        7단계 로직 + 공백 날짜 출력 기능
        """
        if not self.check_connection():
            return None

        # 1. ticker 입력 및 경로 설정
        code = "A" + ticker if not ticker.startswith('A') else ticker
        file_path = os.path.join(self.save_dir, f"{code[1:]}.csv")
        
        if not os.path.exists(file_path):
            print(f"[*] {ticker}: 신규 파일입니다. 전체 데이터를 요청합니다.")
            return self.api.request(code, retrieve_type="2", retrieve_limit=20000, chart_type='m')

        # 2. 마지막 업데이트 날짜 확인
        df = pd.read_csv(file_path)
        if df.empty:
            return self.api.request(code, retrieve_type="2", retrieve_limit=20000, chart_type='m')

        # 3. 데이터 무결성(날짜 기준) 검사
        df = df.drop_duplicates(subset=['date', 'time']).sort_values(['date', 'time'])
        exist_dates = sorted(df['date'].unique())
        
        start_dt = datetime.strptime(str(exist_dates[0]), '%Y%m%d')
        end_dt = datetime.strptime(str(exist_dates[-1]), '%Y%m%d')
        
        # 실제 개장일 리스트 생성 (주말/공휴일 제외)
        all_days = pd.date_range(start=start_dt, end=end_dt)
        valid_biz_days = [
            int(d.strftime('%Y%m%d')) for d in all_days 
            if d.weekday() < 5 and d not in self.kr_holidays
        ]

        # 5. 비어있는 날짜 추출 및 출력
        missing_dates = [d for d in valid_biz_days if d not in exist_dates]
        
        if missing_dates:
            print(f"\n[!] {ticker}: 데이터 공백이 발견되었습니다.")
            print(f"    - 비어있는 날짜 ({len(missing_dates)}일): {missing_dates}")
            print(f"    - 보충 요청을 시작합니다...")
            
            for m_date in missing_dates:
                if not self.check_connection(): break
                gap_df = self.api.request(code, retrieve_type="1", fromDate=m_date, toDate=m_date, chart_type='m')
                if isinstance(gap_df, pd.DataFrame) and not gap_df.empty:
                    df = pd.concat([df, gap_df])
        else:
            print(f"[*] {ticker}: 과거 데이터 내 날짜 공백이 없습니다.")

        # 6. 최신 상태 확인 및 업데이트
        last_date = int(exist_dates[-1])
        today_date = int(datetime.now().strftime('%Y%m%d'))
        
        if last_date < today_date:
            print(f"[*] {ticker}: 최신 데이터({last_date} 이후)를 요청합니다.")
            new_df = self.api.request(code, retrieve_type="1", fromDate=last_date, toDate=today_date, chart_type='m')
            if isinstance(new_df, pd.DataFrame):
                df = pd.concat([df, new_df])
        else:
            print(f"[*] {ticker}: 이미 최신 날짜입니다.")

        return df.drop_duplicates(subset=['date', 'time']).sort_values(['date', 'time'])

# --- 테스트 코드 ---
if __name__ == "__main__":
    updater = MinuteChartUpdater()
    ticker_input = input("업데이트할 종목코드(6자리): ")
    result = updater.get_updated_data(ticker_input)
    
    if result is not None:
        print(f"\n--- {ticker_input} 최종 데이터 요약 ---")
        print(f"전체 행 수: {len(result):,} | 마지막 날짜: {result['date'].max()}")
        print(result.tail(10))