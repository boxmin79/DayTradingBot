import os
import sys
import pandas as pd
import time
from datetime import timedelta

# 프로젝트 루트 경로 추가
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(BASE_DIR)

from Collector.data_pipeline import DataPipeline

def run_all_tickers():
    """
    filtered_tickers.csv에 포함된 모든 종목에 대해 
    data_pipeline.py의 process_ticker를 실행합니다.
    """
    pipeline = DataPipeline()
    
    # 1. 종목 리스트 파일 경로 설정
    ticker_file = os.path.join(BASE_DIR, "data", "ticker", "filtered_tickers.csv")
    
    if not os.path.exists(ticker_file):
        print(f"!!! [오류] 종목 리스트 파일을 찾을 수 없습니다: {ticker_file}")
        return

    # 2. 종목 리스트 로드
    df_tickers = pd.read_csv(ticker_file)
    # 종목 코드를 6자리 문자열로 변환 (예: 5930 -> 005930)
    df_tickers['code'] = df_tickers['code'].astype(str).str.zfill(6)
    
    total_tickers = len(df_tickers)
    print(f"\n--- [전체 파이프라인 시작] 총 {total_tickers}개 종목 ---")
    
    start_time = time.time()

    # 3. 각 종목별로 파이프라인 실행
    for idx, row in df_tickers.iterrows():
        ticker = row['code']
        name = row['name']
        
        current_idx = idx + 1
        progress = (current_idx / total_tickers) * 100
        
        print(f"\n[{current_idx}/{total_tickers}] {progress:.1f}% 진행 중: {name}({ticker})")
        
        try:
            # data_pipeline.py의 핵심 로직 실행
            pipeline.process_ticker(ticker)
        except Exception as e:
            print(f"!!! [오류] {name}({ticker}) 처리 중 예외 발생: {e}")
            continue

        # API 호출 제한을 고려한 짧은 대기 (필요 시 조정)
        time.sleep(0.1)

    # 4. 전체 완료 보고
    elapsed_time = time.time() - start_time
    print("\n" + "="*50)
    print(f"--- [전체 파이프라인 완료] ---")
    print(f"총 처리 종목: {total_tickers}개")
    print(f"총 소요 시간: {str(timedelta(seconds=int(elapsed_time)))}")
    print("="*50)

if __name__ == "__main__":
    run_all_tickers()