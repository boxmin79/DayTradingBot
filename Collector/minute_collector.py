import os
import pandas as pd
from ..API.Daishin.stock_chart import CpStockChart

class MinuteCollector:
    def __init__(self):
        self.api = CpStockChart()
        self.base_dir = os.getcwd()
        
        # 데이터 저장 폴더 (속도가 빠른 PKL 방식)
        self.save_dir = os.path.join(self.base_dir, "data", "chart", "minute")
        self.ticker_path = os.path.join(self.base_dir, "data", "ticker", "filtered_tickers.csv")
        os.makedirs(self.save_dir, exist_ok=True)
        
    def collect_all_tickers(self):

        if not os.path.exists(self.ticker_path):
            print(f"!!! [오류] 종목 리스트 파일이 없습니다: {self.ticker_path}")
            return

        df_tickers = pd.read_csv(self.ticker_path)
        total = len(df_tickers)
        
        for idx, row in df_tickers.iterrows():
            
            ticker_code = str(row['code']).zfill(6)
            ticker_name = row['name']
            
            # 진행률 표시
            progress = ((idx + 1) / total) * 100
            print(f"\n[{progress:6.2f}%] {ticker_name}({ticker_code})")
            
            try:
                # 1. 데이터 업데이트 (최대 20만행)
                df = self._update_single_ticker("A" + ticker_code, target_count=200000)
                print(f"    [완료] {len(df):,} 행 저장됨")
                
            except Exception as e:
                print(f"    !!! {ticker_name} 처리 중 에러: {e}")

    def _update_single_ticker(self, code, target_count=200000):
        """개별 종목 데이터를 수집하고 PKL로 저장"""
        file_path = os.path.join(self.save_dir, f"{code[1:]}.pkl")
        
        # 1. 기존 데이터 로드
        if os.path.exists(file_path):
            existing_df = pd.read_pickle(file_path)
        else:
            existing_df = pd.DataFrame()

        # 2. 최신 데이터 확인 및 실행 여부 결정 (추가된 핵심 로직)
        if not existing_df.empty:
            # 로컬 데이터의 마지막 날짜와 시간 추출
            last_date = existing_df['date'].max()
            last_time = existing_df[existing_df['date'] == last_date]['time'].max()
            
            # 서버의 가장 최신 데이터 1개만 요청하여 비교
            try:
                current_top = self.api.request(
                    code=code,
                    retrieve_type="2",      # "2" = 개수로 받기 (분 단위)
                    retrieve_limit=1,       # 최신 1개만
                    chart_type='m',         # 분 차트
                    interval=1              # 1분 간격
                )
            except Exception:
                current_top = None
            
            if current_top is not None and not current_top.empty:
                curr_date = current_top['date'].iloc[0]
                curr_time = current_top['time'].iloc[0]
                
                # 시점이 완전히 일치하면 수집 중단하고 바로 반환
                if last_date == curr_date and last_time == curr_time:
                    print(f"    [건너뜀] 이미 최신 상태입니다. ({last_date} {last_time})")
                    return existing_df
            
            print(f"    [기존데이터] {len(existing_df):,} 행, 최신: {last_date} {last_time} -> 업데이트 시작")
        else:
            print(f"    [신규수집] 기존 데이터 없음 -> {target_count:,} 행 수집 시작")
        
        
        all_new = [] # 수집된 새로운 데이터 저장 리스트
        is_next = False # 다음 페이지 플래그
        
        # 새로운 데이터 수집 시작
        print(f"    [수집시작] 목표: {target_count:,} 행 확보")
        
        
        # 목표 수량 채울 때까지 반복 수집
        while (len(existing_df) + sum(len(x) for x in all_new)) < target_count:
            df = self.api.request(
                code=code,
                retrieve_type="2",         # "2" = 개수로 받기
                retrieve_limit=2247,       # 최대 2247개
                chart_type='m',            # 분 차트
                interval=1                 # 1분 간격
            )
            
            if df is None or df.empty:
                break
                
            all_new.append(df)
            total_now = len(existing_df) + sum(len(x) for x in all_new)
            print(f"    [수집중] {total_now:,} / {target_count:,} 행 확보 중...", end='\r')
            
            # 더 이상 과거 데이터가 없으면 중단
            if not self.api.obj_stock_chart.Continue:
                break
            is_next = True

        # 새로운 데이터가 있으면 합치기
        if all_new:
            new_data = pd.concat(all_new, ignore_index=True)
            # 기존 데이터와 합친 후 중복 제거 및 정렬
            combined_df = pd.concat([existing_df, new_data], ignore_index=True)
            combined_df.drop_duplicates(subset=['date', 'time'], keep='last', inplace=True)
            combined_df.sort_values(by=['date', 'time'], inplace=True)
            
            # 목표 수량만큼만 유지 (최신 데이터 기준)
            if len(combined_df) > target_count:
                combined_df = combined_df.tail(target_count)
            
            # 파일 저장 및 반환
            combined_df.to_pickle(file_path)
            return combined_df
        
        return existing_df
        