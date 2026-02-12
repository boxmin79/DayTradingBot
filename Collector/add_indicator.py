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

    def add_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        외부에서 받은 2년치 df에 지표를 추가합니다.
        데이터가 너무 적으면 지표 계산 결과가 부정확하므로 종료합니다.
        """
        # 1. 데이터 존재 여부 및 최소 행 수 체크
        # 최소 150행은 있어야 120일 이평선 등 주요 지표가 계산됩니다.
        MIN_REQUIRED_ROWS = 150 
        
        if df is None or df.empty:
            print("[!] 오류: 입력받은 데이터프레임이 비어 있습니다.")
            return pd.DataFrame()

        if len(df) < MIN_REQUIRED_ROWS:
            print(f"[!] 오류: 데이터가 너무 적습니다. (현재: {len(df)}행 / 최소 필요: {MIN_REQUIRED_ROWS}행)")
            print("[*] 지표 계산을 중단하고 빈 데이터프레임을 반환합니다.")
            return pd.DataFrame()

        # 2. 데이터 복사 및 정렬
        # 지표는 순서가 중요하므로 시간순 정렬을 보장합니다.
        combined_df = df.copy()
        sort_cols = ['date', 'time'] if 'time' in combined_df.columns else ['date']
        combined_df = combined_df.sort_values(sort_cols).reset_index(drop=True)

        print(f"[*] {len(combined_df)}행의 데이터를 확인했습니다. 지표 계산을 시작합니다.")

        # 3. 지표 계산 (factory 활용)
        try:
            combined_df = IndicatorFactory.add_all_indicators(combined_df)
            combined_df = IndicatorFactory.add_custom_indicators(combined_df)
        except Exception as e:
            print(f"[!] 지표 계산 중 오류 발생: {e}")
            return pd.DataFrame()

        return combined_df

# 테스트 코드
if __name__ == "__main__":
    # 삼성전자 분봉 parquet파일을 로드
    df = pd.read_parquet(f"data/chart/minute/005930.parquet")
    # df에 치표 추가
    df = ChartIndicatorAdder().add_indicators(df)
    print("분봉 테스트 결과:")
    print(df)
    
    #삼성전자 일봉도 지표 추가
    daily_df = pd.read_parquet(f"data/chart/daily/005930.parquet")
    daily_df = ChartIndicatorAdder().add_indicators(daily_df)
    print("일봉 테스트 결과:")
    print(daily_df)
    
    