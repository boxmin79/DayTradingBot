import os
import sys
import pandas as pd
from datetime import datetime

# 프로젝트 루트 경로 추가
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(BASE_DIR)

from Collector.update_minute_chart import MinuteChartUpdater
from Collector.add_indicator import ChartIndicatorAdder

class DataPipeline:
    def __init__(self):
        self.updater = MinuteChartUpdater()
        self.indicator_adder = ChartIndicatorAdder()
        self.min_dir = os.path.join(BASE_DIR, "data", "chart", "minute")
        self.daily_dir = os.path.join(BASE_DIR, "data", "chart", "daily")
        
        # 데이터 유지 설정 (2년치)
        self.min_target_count = 200000 
        self.daily_target_count = 500

    def process_ticker(self, ticker):
        print(f"\n[*] {ticker} 데이터 파이프라인 시작...")

        # 1. 업데이트된 부분만 분봉 df 생성 (무결성 검사 및 보충 포함)
        # update_minute_chart.py의 로직을 사용하여 최신/보충된 전체 DF를 가져옵니다.
        full_min_df = self.updater.get_updated_data(ticker)
        if full_min_df is None or full_min_df.empty:
            print(f"[!] {ticker}: 업데이트된 데이터를 가져오지 못했습니다.")
            return

        # 2. 수집된 분봉을 일봉으로 변환후 일봉 df 생성
        print(f"[*] {ticker}: 일봉 변환 중...")
        full_daily_df = self._convert_to_daily(full_min_df)

        # 3 & 4. 분봉과 일봉 df에 지표 추가
        # (기존 차트에 지표가 없더라도 add_indicators_with_history factory를 통해 전체 계산)
        print(f"[*] {ticker}: 기술적 지표 계산 중...")
        full_min_df = self.indicator_adder.add_indicators_with_history(ticker, full_min_df)
        full_daily_df = self.indicator_adder.add_indicators_with_history(ticker, full_daily_df)

        # 6. 2년치가 넘는 데이터 삭제 (최신 데이터 기준 cut)
        full_min_df = full_min_df.tail(self.min_target_count)
        full_daily_df = full_daily_df.tail(self.daily_target_count)

        # 5. 업데이트된(지표 추가/데이터 정리 완료) 차트를 CSV에 저장
        min_path = os.path.join(self.min_dir, f"{ticker}.csv")
        daily_path = os.path.join(self.daily_dir, f"{ticker}.csv")

        full_min_df.to_csv(min_path, index=False, encoding='utf-8-sig')
        full_daily_df.to_csv(daily_path, index=False, encoding='utf-8-sig')

        print(f"[OK] {ticker}: 분봉({len(full_min_df)}행), 일봉({len(full_daily_df)}행) 저장 완료.")

    def _convert_to_daily(self, m_df):
        """분봉 -> 일봉 변환 로직"""
        df = m_df.copy()
        df['datetime'] = pd.to_datetime(df['date'].astype(str) + df['time'].astype(str).str.zfill(4), format='%Y%m%d%H%M')
        df.set_index('datetime', inplace=True)
        
        d_df = df.resample('1D').agg({
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'volume': 'sum'
        }).dropna()
        
        d_df['date'] = d_df.index.strftime('%Y%m%d').astype(int)
        return d_df.reset_index(drop=True)

# --- 실행 테스트 코드 ---
if __name__ == "__main__":
    pipeline = DataPipeline()
    ticker_input = input("파이프라인을 실행할 종목코드를 입력하세요: ")
    pipeline.process_ticker(ticker_input)