import os
import sys
import pandas as pd
from datetime import datetime, timedelta
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

    def _combine_datetime(self, df):
        """date(int)와 time(int) 컬럼을 결합하여 datetime 인덱스로 변환"""
        if df is None or df.empty:
            return df
        
        # 날짜와 시간을 문자열로 변환 후 결합 (시간은 4자리 보정)
        dt_str = df['date'].astype(str) + df['time'].astype(str).str.zfill(4)
        df['datetime'] = pd.to_datetime(dt_str, format='%Y%m%d%H%M')
        
        # 인덱스 설정 후 기존 컬럼 제거
        df = df.set_index('datetime').sort_index()
        df.drop(['date', 'time'], axis=1, inplace=True, errors='ignore')
        return df

    def get_market_days(self, start, end):
        """실제 한국 거래소 영업일 리스트 추출"""
        try:
            days = stock.get_market_ohlcv(start, end, "005930")
            return pd.to_datetime(days.index)
        except Exception as e:
            print(f"[!] 영업일 리스트 획득 실패: {e}")
            return pd.DatetimeIndex([])

    def request_until_count(self, code, target_count):
        """목표 개수를 채울 때까지 연속 쿼리 수행 후 datetime 변환"""
        all_dfs = []
        current_count = 0
        is_continue = False
        
        print(f"[*] {code}: 신규 데이터 요청 시작 (목표: {target_count}개)...")
        
        while current_count < target_count:
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
            
            if not self.api.obj_stock_chart.Continue:
                break
            
            is_continue = True
            time.sleep(0.05)
            
        if not all_dfs:
            return None
            
        combined_df = pd.concat(all_dfs, ignore_index=True)
        return self._combine_datetime(combined_df)

    def get_updated_data(self, ticker, listing_date=None, save=False):
        code = "A" + ticker if not ticker.startswith('A') else ticker
        file_path = os.path.join(self.save_dir, f"{code[1:]}.parquet")
        
        now = datetime.now()
        today_str = now.strftime('%Y%m%d')
        limit_dt = now - pd.DateOffset(years=2)
        
        if listing_date:
            try:
                l_dt = pd.to_datetime(str(listing_date), format='%Y%m%d')
                if l_dt > limit_dt:
                    limit_dt = l_dt
                    print(f"[*] {ticker}: 수집 기준일을 상장일({l_dt.date()})로 조정합니다.")
            except Exception as e:
                print(f"[!] 상장일 파싱 에러: {e}")

        df = pd.DataFrame()
        new_data_list = []

        # 1. 기존 데이터 로드 (이미 datetime 인덱스인 상태) 
        if os.path.exists(file_path):
            try:
                df = pd.read_parquet(file_path)
                # 인덱스가 datetime이 아닌 경우를 대비해 보정
                if not isinstance(df.index, pd.DatetimeIndex):
                    df = self._combine_datetime(df)
                print(f"[*] {ticker}: 기존 데이터 로드 ({len(df)}행).")
            except Exception as e:
                print(f"[!] 로드 에러: {e}")

        # 2. 신규 수집
        if df.empty:
            full_df = self.request_until_count(code, target_count=200000)
            if full_df is not None:
                full_df = full_df[full_df.index >= limit_dt]
                if save:
                    full_df.to_parquet(file_path, compression='snappy')
                return full_df
            return None

        # 3. 결측일 보충 로직
        existing_dates = pd.to_datetime(df.index.date).unique()
        market_days = self.get_market_days(limit_dt.strftime('%Y%m%d'), today_str)
        missing_days = market_days.difference(existing_dates)
        
        if not missing_days.empty:
            for i, m_day in enumerate(missing_days):
                t_date = int(m_day.strftime('%Y%m%d'))
                chunk = self.api.request(code, retrieve_type="1", fromDate=t_date, toDate=t_date, chart_type='m')
                if isinstance(chunk, pd.DataFrame) and not chunk.empty:
                    new_data_list.append(self._combine_datetime(chunk))
                
                if (i + 1) % 20 == 0:
                    print(f"    - 보충 중: {i+1}/{len(missing_days)}일 완료...", end='\r')

        # 4. 최신 데이터 (오늘 장중 등)
        if df.index.max().date() < now.date():
            recent_df = self.api.request(code, retrieve_type="2", retrieve_limit=2000, chart_type='m')
            if isinstance(recent_df, pd.DataFrame) and not recent_df.empty:
                new_data_list.append(self._combine_datetime(recent_df))

        # 5. 병합 및 최종 필터링
        if new_data_list:
            new_data_df = pd.concat(new_data_list)
            df = pd.concat([df, new_data_df])
        
        df = df[~df.index.duplicated(keep='last')].sort_index()
        df = df[df.index >= limit_dt]

        # 6. 장 마감(15:30) 전 오늘 데이터 삭제 로직
        if now.date() in df.index.date:
            last_dt_today = df[df.index.date == now.date()].index.max()
            # 15시 30분 미만이면 삭제
            if last_dt_today.time() < datetime.strptime("15:30", "%H:%M").time():
                print(f"[*] {ticker}: 오늘 데이터가 장 마감 전이므로 제외합니다.")
                df = df[df.index.date != now.date()]

        if save:
            df.to_parquet(file_path, compression='snappy')
            print(f"[✔] {ticker}: 업데이트 완료. (총 {len(df)}행)")
        
        return df

if __name__ == "__main__":
    updater = MinuteChartUpdater()
    updater.get_updated_data("005930", listing_date="19750611", save=True)