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

    def get_market_days(self, start, end):
        """실제 한국 거래소 영업일 리스트 추출"""
        try:
            # pykrx가 YYYYMMDD 문자열을 받음
            days = stock.get_market_ohlcv(start, end, "005930")
            return pd.to_datetime(days.index)
        except Exception as e:
            print(f"[!] 영업일 리스트 획득 실패: {e}")
            return pd.DatetimeIndex([])

    def request_until_count(self, code, target_count):
        """목표 개수를 채울 때까지 연속 쿼리를 수행하는 헬퍼 함수 (신규 수집용)"""
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
            
        return pd.concat(all_dfs, ignore_index=True)

    def get_updated_data(self, ticker, listing_date=None, save=False):
        """
        :param ticker: 종목코드
        :param listing_date: 상장일 (YYYYMMDD 문자열 또는 숫자)
        :param save: 저장 여부
        """
        code = "A" + ticker if not ticker.startswith('A') else ticker
        file_path = os.path.join(self.save_dir, f"{code[1:]}.parquet")
        
        now = datetime.now()
        today_str = now.strftime('%Y%m%d')
        
        # 기본: 2년 전 기준 날짜
        limit_dt = now - pd.DateOffset(years=2)
        
        # [수정된 로직] 상장일이 2년 이내인 경우 기준일(limit_dt)을 상장일로 변경
        if listing_date:
            try:
                l_dt = pd.to_datetime(str(listing_date), format='%Y%m%d')
                if l_dt > limit_dt:
                    limit_dt = l_dt
                    print(f"[*] {ticker}: 상장일({listing_date})이 2년 이내입니다. 수집 기준일을 {l_dt.strftime('%Y-%m-%d')}로 조정합니다.")
            except Exception as e:
                print(f"[!] {ticker}: 상장일 파싱 에러({listing_date}) -> 기본 2년 전 기준으로 진행. ({e})")

        limit_date_str = limit_dt.strftime('%Y%m%d')
        limit_date_int = int(limit_date_str)

        df = pd.DataFrame()
        new_data_list = []

        # 1. 기존 데이터 로드
        if os.path.exists(file_path):
            try:
                df = pd.read_parquet(file_path, engine='fastparquet')
                print(f"[*] {ticker}: 기존 데이터 로드 ({len(df)}행).")
            except Exception as e:
                print(f"[!] 파일 로드 에러({ticker}): {e}. 새로 수집합니다.")
                df = pd.DataFrame()

        # 2. 신규 종목이거나 파일이 깨진 경우 -> 통으로 수집 후 종료
        if df.empty:
            print(f"[*] {ticker}: 데이터 없음. 신규 수집.")
            # 상장일이 2년 이내면 굳이 20만개를 요청할 필요 없이 적절히 줄여도 되지만, 
            # API가 알아서 끊어주므로 넉넉하게 요청
            full_df = self.request_until_count(code, target_count=200000)
            if full_df is not None and not full_df.empty:
                # 2년치(혹은 상장일 이후) 필터링
                if 'date' in full_df.columns:
                     full_df = full_df[full_df['date'].astype(int) >= limit_date_int]
                
                if save:
                    full_df = full_df.sort_values(['date', 'time']).drop_duplicates(subset=['date', 'time'])
                    full_df.to_parquet(file_path, engine='fastparquet', compression='snappy', index=False)
                return full_df
            return None

        # ------------------------------------------------------------------
        # 로드된 기존 데이터(df)를 기준으로 결측치(구멍) 계산
        # ------------------------------------------------------------------
        
        if df['date'].dtype == 'object':
            df['date'] = df['date'].astype(int)
            
        existing_dates = pd.to_datetime(df['date'].unique().astype(str), format='%Y%m%d')
        
        # 기준일(limit_dt) ~ 오늘까지의 영업일
        market_days = self.get_market_days(limit_date_str, today_str)
        
        # 로컬에 없는 날짜 (결측일) 추출
        missing_days = market_days.difference(existing_dates)
        
        # 3. 결측일 데이터 보충
        if not missing_days.empty:
            print(f"[*] {ticker}: {len(missing_days)}일치 데이터 보충 ({missing_days.min().strftime('%Y-%m-%d')} ~ )...")
            
            for i, m_day in enumerate(missing_days):
                t_date = int(m_day.strftime('%Y%m%d'))
                
                chunk = self.api.request(code, retrieve_type="1", fromDate=t_date, toDate=t_date, chart_type='m')
                if isinstance(chunk, pd.DataFrame) and not chunk.empty:
                    new_data_list.append(chunk)
                
                if (i + 1) % 20 == 0:
                    print(f"    - 보충 진행: {i+1}/{len(missing_days)}일 완료...", end='\r')
            print("") 

        # 4. 최신 데이터 업데이트 (오늘 장중 데이터 등)
        last_local_date = df['date'].max()
        if last_local_date < int(today_str):
            print(f"[*] {ticker}: 최신 데이터 확인 중...")
            recent_df = self.api.request(code, retrieve_type="2", retrieve_limit=2000, chart_type='m')
            if isinstance(recent_df, pd.DataFrame) and not recent_df.empty:
                new_data_list.append(recent_df)

        # 5. 병합 및 저장
        if new_data_list:
            print(f"[*] {ticker}: {len(new_data_list)}개의 신규 청크 병합 중...")
            new_data_df = pd.concat(new_data_list, ignore_index=True)
            df = pd.concat([df, new_data_df], ignore_index=True)
        
        df = df.drop_duplicates(subset=['date', 'time']).sort_values(['date', 'time'])
        
        # 기준일(2년 전 또는 상장일) 이전 데이터 삭제
        df = df[df['date'] >= limit_date_int]

        # ------------------------------------------------------------------
        # [추가된 로직] 오늘 데이터가 장 마감(15:30)까지 채워지지 않았으면 삭제
        # ------------------------------------------------------------------
        today_int = int(today_str)
        
        # 데이터프레임에 오늘 날짜가 존재하는지 확인
        if today_int in df['date'].values:
            # 오늘 날짜 데이터 중 가장 마지막 시간 확인
            last_time_today = df[df['date'] == today_int]['time'].max()
            
            # 마지막 시간이 1530 (15시 30분) 미만이라면 -> 장 마감 전 데이터로 간주하고 삭제
            if last_time_today < 1530:
                print(f"[*] {ticker}: 오늘({today_int}) 데이터가 장 마감(15:30) 전({last_time_today})이므로 저장 목록에서 제외합니다.")
                df = df[df['date'] != today_int]

        if save:
            df.to_parquet(file_path, engine='fastparquet', compression='snappy', index=False)
            print(f"[✔] {ticker}: 업데이트 완료. 총 {len(df)}행.")
        
        return df

if __name__ == "__main__":
    updater = MinuteChartUpdater()
    # 테스트 (상장일 인자 추가됨)
    updater.get_updated_data("005930", listing_date="19750611", save=True)