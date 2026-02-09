import os
import sys

# 프로젝트 루트 경로를 sys.path에 추가 (API 패키지를 찾기 위함)
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
from API.Daishin.stock_chart import CpStockChart

class MinuteCollector:
    def __init__(self):
        self.api = CpStockChart()
        # 실행 위치와 상관없이 프로젝트 루트를 기준으로 경로 설정
        self.base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        
        # 데이터 저장 경로 (CSV 파일로 저장됨)
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

    def verify_data(self, code):
        """수집된 데이터의 무결성(시간 연속성) 검사"""
        file_path = os.path.join(self.save_dir, f"{code[1:]}.csv")
        if not os.path.exists(file_path):
            return
            
        df = pd.read_csv(file_path)
        if df.empty: return

        # 날짜+시간 컬럼 생성 및 정렬
        df['datetime'] = pd.to_datetime(df['date'].astype(str) + df['time'].astype(str).str.zfill(4), format='%Y%m%d%H%M')
        df = df.sort_values('datetime')
        
        # 시간 차이 계산 (장중 데이터만 고려해야 하므로 단순 diff는 부정확할 수 있으나, 큰 공백 발견용으로 유용)
        time_diff = df['datetime'].diff()
        
        # 10분 이상 데이터가 빈 구간 찾기 (장 마감/시작 제외)
        gaps = time_diff[time_diff > pd.Timedelta(minutes=10)]
        if not gaps.empty:
            print(f"    [주의] {code} 데이터에 {len(gaps)}개의 큰 시간 공백이 발견되었습니다.")

    def _update_single_ticker(self, code, target_count=200000):
        """개별 종목 데이터를 수집하고 CSV로 저장"""
        file_path = os.path.join(self.save_dir, f"{code[1:]}.csv")
        
        # 1. 기존 데이터 로드
        if os.path.exists(file_path):
            existing_df = pd.read_csv(file_path)
        else:
            existing_df = pd.DataFrame()

        # 2. 최신 데이터 확인 및 실행 여부 결정
        if not existing_df.empty:
            # 로컬 데이터의 마지막 날짜와 시간 추출
            last_date = existing_df['date'].max()
            last_time = existing_df[existing_df['date'] == last_date]['time'].max()
            
            print(f"    [업데이트] 기존: {last_date} {last_time} -> 데이터 검증 및 추가")
        else:
            print(f"    [신규수집] 기존 데이터 없음 -> 2년치({target_count:,}행) 수집 시작")
        
        
        all_new = [] # 수집된 새로운 데이터 저장 리스트
        
        # 3. 데이터 수집 (최신순으로 가져옴)
        is_first = True
        while True:
            df = self.api.request(
                code=code,
                retrieve_type="2",         # "2" = 개수로 받기
                retrieve_limit=2247,       # 최대 2247개
                chart_type='m',            # 분 차트
                interval=1,                # 1분 간격
                continue_query=not is_first
            )
            is_first = False
            
            if not isinstance(df, pd.DataFrame) or df.empty:
                break
            
            # 중복 수집 방지 (API 커서 리셋 문제 대응)
            if all_new:
                prev_first = all_new[-1].iloc[0]
                curr_first = df.iloc[0]
                if prev_first['date'] == curr_first['date'] and prev_first['time'] == curr_first['time']:
                    break

            all_new.append(df)
            current_count = sum(len(x) for x in all_new)
            
            # 기존 데이터와 연결 확인
            if not existing_df.empty:
                min_date = df['date'].min()
                min_time = df[df['date'] == min_date]['time'].min()
                
                last_date = existing_df['date'].max()
                last_time = existing_df[existing_df['date'] == last_date]['time'].max()
                
                # 가져온 데이터의 가장 과거가 기존 데이터의 최신보다 과거거나 같으면 연결됨
                if min_date < last_date or (min_date == last_date and min_time <= last_time):
                    # [중요] 연결되었더라도, 합친 개수가 목표(20만개)보다 적으면 계속 과거 데이터를 수집해야 함
                    if (len(existing_df) + current_count) >= target_count:
                        break
            
            # 목표 수량 도달 시 중단
            # (기존 데이터와 연결되지 않았더라도, 이미 목표치(2년치)를 모두 확보했다면 더 이상 과거 데이터를 수집할 필요 없음)
            if current_count >= target_count:
                break
            
            print(f"    [수집중] {current_count:,} 행 확보 중...", end='\r')
            
            # 더 이상 과거 데이터가 없으면 중단
            if not self.api.obj_stock_chart.Continue:
                break

        # 4. 병합 및 정리
        if all_new:
            new_data = pd.concat(all_new, ignore_index=True)
            # 기존 데이터와 합친 후 중복 제거 및 정렬
            combined_df = pd.concat([existing_df, new_data], ignore_index=True)
            combined_df.drop_duplicates(subset=['date', 'time'], keep='last', inplace=True)
            combined_df.sort_values(by=['date', 'time'], inplace=True)
            
            # 목표 수량(2년치)만큼만 유지 (최신 데이터 기준)
            if len(combined_df) > target_count:
                # 단순히 개수로 자르면 가장 과거 날짜의 데이터가 중간에 잘릴 수 있음
                # 따라서 남길 데이터의 시작 날짜를 기준으로 전체 하루치를 포함하도록 함
                cutoff_idx = len(combined_df) - target_count
                cutoff_date = combined_df.iloc[cutoff_idx]['date']
                combined_df = combined_df[combined_df['date'] >= cutoff_date]
            
            # 파일 저장 및 반환
            combined_df.to_csv(file_path, index=False)
            return combined_df
        
        return existing_df
        
# 테스트 
if __name__ == "__main__":
    collector = MinuteCollector()
    collector.collect_all_tickers()