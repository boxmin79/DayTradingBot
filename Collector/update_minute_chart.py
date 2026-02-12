import os
import sys
import pandas as pd
from datetime import datetime
from pykrx import stock
import time

# 프로젝트 루트 경로 추가
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(BASE_DIR)

from API.Daishin.stock_chart import CpStockChart

class MinuteChartUpdater:
    def __init__(self):
        self.api = CpStockChart()
        self.save_dir = os.path.join(BASE_DIR, "data", "chart", "minute")
        os.makedirs(self.save_dir, exist_ok=True)

    def get_market_days(self, start, end):
        """실제 한국 거래소 영업일 리스트 추출"""
        try:
            df = stock.get_market_ohlcv(str(start), str(end), "005930")
            return pd.to_datetime(df.index)
        except Exception as e:
            print(f"[!] 영업일 리스트 획득 실패: {e}")
            return pd.DatetimeIndex([])

    def request_until_count(self, code, target_count):
        """목표 개수를 채울 때까지 연속 쿼리를 수행하는 헬퍼 함수"""
        all_dfs = []
        current_count = 0
        is_continue = False
        
        print(f"[*] {code}: 데이터 요청 시작 (목표: {target_count}개)...")
        
        while current_count < target_count:
            # CpStockChart의 request 호출
            df = self.api.request(
                code=code, 
                retrieve_type="2", 
                retrieve_limit=target_count, 
                chart_type='m',
                continue_query=is_continue
            )
            
            if df is None or (isinstance(df, bool) and df is False) or df.empty:
                break
                
            all_dfs.append(df)
            current_count += len(df)
            print(f"    - 현재 수집량: {current_count}행 수신 중...", end="\r")
            
            # 더 이상 가져올 데이터가 없으면 중단
            if not self.api.obj_stock_chart.Continue:
                break
            
            is_continue = True
            # API 호출 제한을 고려한 짧은 대기 (필요시)
            time.sleep(0.05)
            
        if not all_dfs:
            return None
            
        return pd.concat(all_dfs, ignore_index=True)

    def get_updated_data(self, ticker, save=False):
        code = "A" + ticker if not ticker.startswith('A') else ticker
        file_path = os.path.join(self.save_dir, f"{code[1:]}.parquet")
        
        now = datetime.now()
        today_int = int(now.strftime('%Y%m%d'))
        limit_dt = now - pd.DateOffset(years=2)
        limit_date_int = int(limit_dt.strftime('%Y%m%d'))

        # 1. 기존 데이터 로드
        if os.path.exists(file_path):
            df = pd.read_parquet(file_path, engine='fastparquet')
            print(f"[*] {ticker}: 기존 데이터 로드 ({len(df)}행).")
            # 최신 데이터 업데이트 (개수 기반으로 빠르게 수집)
            new_df = self.api.request(code, retrieve_type="2", retrieve_limit=3000, chart_type='m')
            if isinstance(new_df, pd.DataFrame) and not new_df.empty:
                df = pd.concat([df, new_df], ignore_index=True)
        else:
            # 파일이 없으면 2년치 전체 요청 (연속 쿼리 사용)
            print(f"[*] {ticker}: 신규 종목입니다. 전체 데이터를 요청합니다.")
            df = self.request_until_count(code, target_count=200000)

        if df is None or df.empty:
            print(f"[!] {ticker}: 데이터를 확보할 수 없습니다.")
            return None

        # 2. 비어있는 날짜 체크 (수정된 핵심 로직)
        df['date'] = df['date'].astype(int)
        df = df.drop_duplicates(subset=['date', 'time']).sort_values(['date', 'time'])
        
        # [수정] 체크 시작점을 데이터의 최소 날짜가 아닌 '2년 전(limit_date_int)'으로 설정
        # 이렇게 해야 '1주일 전 ~ 2년 전' 사이의 공백을 찾아냅니다.
        existing_dates = pd.to_datetime(df['date'].unique().astype(str), format='%Y%m%d')
        market_days = self.get_market_days(str(limit_date_int), str(today_int))
        missing_days = market_days.difference(existing_dates)

        # 3. 누락된 날짜 보충 (날짜 기반 정밀 요청)
        if not missing_days.empty:
            print(f"[*] {ticker}: {len(missing_days)}일의 누락 데이터(과거+중간)를 발견했습니다.")
            print(f"    - 범위: {missing_days.min().date()} ~ {missing_days.max().date()}")
            
            new_chunks = []
            # 너무 많은 날짜를 요청하면 시간이 걸리므로 진행 상황 표시
            for i, m_day in enumerate(missing_days):
                t_date = int(m_day.strftime('%Y%m%d'))
                # 개별 날짜 요청
                chunk = self.api.request(code, retrieve_type="1", fromDate=t_date, toDate=t_date, chart_type='m')
                
                if isinstance(chunk, pd.DataFrame) and not chunk.empty:
                    new_chunks.append(chunk)
                
                # 로그 출력 (10일 단위)
                if (i + 1) % 10 == 0:
                    print(f"    - 진행률: {i + 1}/{len(missing_days)}일 완료...")

            if new_chunks:
                df = pd.concat([df] + new_chunks, ignore_index=True)

        # 4. 최종 정제 및 저장 (2년치 엄격 유지)
        df = df.drop_duplicates(subset=['date', 'time']).sort_values(['date', 'time'])
        df = df[df['date'] >= limit_date_int].reset_index(drop=True)

        if save:
            df.to_parquet(file_path, engine='fastparquet', compression='snappy', index=False)
            print(f"[✔] {ticker}: 최종 {len(df)}행 업데이트 완료 (2년치 확보)")
        
        return df

if __name__ == "__main__":
    updater = MinuteChartUpdater()
    # 테스트 종목
    df = updater.get_updated_data("005930", save=True)
    print(df)