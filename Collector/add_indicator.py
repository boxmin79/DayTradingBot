import pandas as pd
import os
import sys

# 프로젝트 루트 경로 추가
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(BASE_DIR)

from Indicators.factory import IndicatorFactory

class ChartIndicatorAdder:
    def __init__(self):
        self.min_dir = os.path.join(BASE_DIR, "data", "chart", "minute")
        self.daily_dir = os.path.join(BASE_DIR, "data", "chart", "daily")

    def add_indicators_with_history(self, ticker: str, df: pd.DataFrame) -> pd.DataFrame:
        """
        기존 데이터와 합쳐 지표를 계산한 후, 
        새로 추가된 데이터(입력받은 df의 기간)만 반환합니다.
        """
        if df is None or df.empty:
            return df

        # 1. 주기 판별 및 파일 경로 설정
        is_minute = 'time' in df.columns
        folder_path = self.min_dir if is_minute else self.daily_dir
        file_path = os.path.join(folder_path, f"{ticker}.csv")

        # 입력 데이터의 시작점 기록 (나중에 잘라낼 기준)
        # 분봉이면 날짜와 시간을 모두 고려, 일봉이면 날짜만 고려
        start_marker = df.iloc[0][['date', 'time']] if is_minute else df.iloc[0]['date']

        # 2. 지표 계산을 위한 과거 데이터 보충 (최소 150행 확보 권장)
        combined_df = df.copy()
        if os.path.exists(file_path):
            history_df = pd.read_csv(file_path)
            # 중복 제거 및 정렬
            combined_df = pd.concat([history_df, df]).drop_duplicates(
                subset=['date', 'time'] if is_minute else ['date']
            ).sort_values(['date', 'time'] if is_minute else ['date']).reset_index(drop=True)

        # 3. 지표 추가 (factory.py 활용)
        combined_df = IndicatorFactory.add_all_indicators(combined_df)
        combined_df = IndicatorFactory.add_custom_indicators(combined_df)

        # 5. 새로 추가된 데이터 부분만 필터링하여 반환
        if is_minute:
            # 날짜와 시간이 입력 데이터의 시작점보다 크거나 같은 데이터만 추출
            mask = (combined_df['date'] > start_marker['date']) | \
                   ((combined_df['date'] == start_marker['date']) & (combined_df['time'] >= start_marker['time']))
            result_df = combined_df[mask]
        else:
            # 날짜가 입력 데이터의 시작일보다 크거나 같은 데이터만 추출
            result_df = combined_df[combined_df['date'] >= start_marker]

        return result_df.reset_index(drop=True)

# --- 테스트 코드 ---
if __name__ == "__main__":
    adder = ChartIndicatorAdder()
    
    # 1월 1일부터 2월 10일까지 이미 데이터가 저장되어 있다고 가정하고,
    # 2월 11일(새 데이터)만 입력했을 때 지표가 계산되어 나오는지 테스트
    sample_new_data = pd.DataFrame({
        'date': [20260211],
        'open': [75000],
        'high': [76000],
        'low': [74500],
        'close': [75500],
        'volume': [1000000]
    })

    ticker_input = "005930"
    # 실제 환경에서는 data/chart/daily/005930.csv가 있어야 과거 데이터를 읽어옴
    result = adder.add_indicators_with_history(ticker_input, sample_new_data)
    
    if result is not None:
        print(f"--- {ticker_input} 신규 데이터 지표 반환 결과 ---")
        # 입력한 2월 11일치 데이터만 출력되지만, ma20 등은 과거 데이터를 참조해 계산되어 있음
        print(result)