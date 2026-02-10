import os
import pandas as pd
from Strategy.volatility_breakout import VolatilityBreakout
from BackTest.volatility_backtest import VolatilityBacktest

def main():
    # 1. 전략 및 백테스트 엔진 설정
    # k값은 나중에 상관관계 분석을 통해 최적화할 대상입니다.
    strategy = VolatilityBreakout(k=0.5) 
    backtester = VolatilityBacktest(strategy)

    # 2. 데이터 경로 설정
    minute_data_path = "data/chart/minute"
    daily_data_path = "data/chart/daily"

    # 3. 분봉 폴더 내의 모든 종목 코드 가져오기 (.csv 제외)
    file_list = [f for f in os.listdir(minute_data_path) if f.endswith('.csv')]
    tickers = [f.replace('.csv', '') for f in file_list]

    print(f"[*] 총 {len(tickers)}개 종목의 백테스트를 시작합니다.")

    # 4. 종목별 루프 가동
    for i, ticker in enumerate(tickers):
        try:
            # 분봉 및 일봉 데이터 로드
            m_df = pd.read_csv(os.path.join(minute_data_path, f"{ticker}.csv"))
            d_df = pd.read_csv(os.path.join(daily_data_path, f"{ticker}.csv"))

            # 일봉 데이터에 'date' 컬럼 추가 (datetime: YYYY-MM-DD -> date: YYYYMMDD)
            if 'datetime' in d_df.columns and 'date' not in d_df.columns:
                d_df['date'] = d_df['datetime'].astype(str).str.replace('-', '').astype(int)

            # 백테스트 실행 (상관관계 분석 파일이 여기서 생성됨)
            backtester.run(ticker, m_df, d_df)
            
            if (i + 1) % 10 == 0:
                print(f"[*] 현재 진행 상황: {i + 1}/{len(tickers)} 완료")

        except Exception as e:
            print(f"[!] {ticker} 처리 중 오류 발생: {e}")
            continue

    # 5. 전 종목 완료 후 통합 요약 보고서(summary.csv) 저장
    backtester.save_final_summary()
    print("\n" + "="*50)
    print("[*] 모든 백테스트 및 분석 데이터 생성이 완료되었습니다.")
    print(f"[*] 결과 확인: data/backtest/result/")
    print("="*50)

if __name__ == "__main__":
    main()