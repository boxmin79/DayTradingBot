import os
import sys
import pandas as pd
from tqdm import tqdm
from multiprocessing import Pool, cpu_count

# 1. 프로젝트 루트 경로 등록
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.dirname(CURRENT_DIR)
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from BackTest.volatility_backtest import VolatilityBacktest
from BackTest.analyzer import BacktestAnalyzer
from Strategy.volatility_breakout import VolatilityBreakout

def worker(ticker):
    """프로세스별 독립적 백테스트 실행 및 통계 추출"""
    try:
        # 각 프로세스 내부에서 객체를 독립적으로 생성
        strategy = VolatilityBreakout(k=0.5)
        backtester = VolatilityBacktest(strategy)
        analyzer = BacktestAnalyzer(strategy.name)
        
        minute_dir = os.path.join(BASE_DIR, "data", "chart", "minute")
        daily_dir = os.path.join(BASE_DIR, "data", "chart", "daily")
        
        m_path = os.path.join(minute_dir, f"{ticker}.parquet")
        d_path = os.path.join(daily_dir, f"{ticker}.parquet")
        
        if os.path.exists(m_path) and os.path.exists(d_path):
            m_df = pd.read_parquet(m_path)
            d_df = pd.read_parquet(d_path)
            
            # 1. 백테스트 실행
            success = backtester.run(ticker, m_df, d_df, save=True)
            
            if success:
                trade_df = analyzer.load_trade_details(ticker)
                if trade_df is not None and not trade_df.empty:
                    # [중요 수정] 반환값이 (stats, df) 튜플이므로 stats만 추출
                    stats, _ = analyzer.calculate_statistics(trade_df) # 
                    return stats
    except Exception:
        return None
    return None

def run_batch_backtest():
    ticker_file = os.path.join(BASE_DIR, "data", "ticker", "filtered_tickers.parquet")
    
    if not os.path.exists(ticker_file):
        print(f" 에러: {ticker_file} 파일이 없습니다.")
        return
    
    # 2. 대상 종목 로드
    tickers_df = pd.read_parquet(ticker_file)
    ticker_list = tickers_df['code'].tolist() if 'code' in tickers_df.columns else tickers_df.index.tolist()

    print(f"[*] 총 {len(ticker_list)}개 종목 멀티 프로세스 백테스트 시작...")
    num_cores = cpu_count()
    print(f"[*] 활용 프로세스 수: {num_cores}")

    # 3. 병렬 처리 실행
    with Pool(processes=num_cores) as pool:
        # 결과를 리스트로 취합
        results = list(tqdm(pool.imap(worker, ticker_list), total=len(ticker_list), desc="Parallel Backtesting"))

    # 4. 결과 통합 및 저장
    all_stats = [res for res in results if res is not None]

    if all_stats:
        summary_df = pd.DataFrame(all_stats)
        
        # 수익률 기준 정렬
        summary_df = summary_df.sort_values(by='Total Return (%)', ascending=False)
        
        # 저장 경로 설정
        temp_analyzer = BacktestAnalyzer(VolatilityBreakout().name)
        save_path = os.path.join(temp_analyzer.result_dir, "total_performance_summary.parquet")
        
        # 결과 저장
        summary_df.to_parquet(save_path, engine='fastparquet', compression='snappy', index=False)
        
        # 5. 결과 출력 (신규 추가된 월별 승률 등 포함)
        print("\n" + "="*80)
        print(f"[*] 분석 완료: {len(all_stats)} 종목 성공")
        print(f"[*] 통합 통계 저장: {save_path}")
        print("-" * 80)
        
        # 출력 컬럼 선택 (월별 지표 포함 가능)
        display_cols = ['Ticker', 'Total Return (%)', 'Profit Factor', 'MDD (%)', 'Avg Monthly (%)']
        available_cols = [c for c in display_cols if c in summary_df.columns]
        print(summary_df[available_cols].head(10))
        print("="*80)
    else:
        print("\n[!] 유효한 결과가 없습니다.")

if __name__ == "__main__":
    run_batch_backtest()