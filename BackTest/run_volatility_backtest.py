import pandas as pd
import os
import glob
import time
from datetime import datetime
from VolatilityBacktestByVBT import VolatilityBacktester

def run_mass_backtest():
    """
    VolatilityBacktester 클래스를 사용하여 전 종목 백테스트를 수행하고
    결과를 지정된 경로에 저장합니다.
    """
    # 1. 백테스터 객체 초기화
    tester = VolatilityBacktester()
    
    # 2. 저장 경로 설정
    TRADES_PATH = "data/backtest/volatility/result"
    SUMMARY_PATH = "data/backtest/volatility/summary"
    
    os.makedirs(TRADES_PATH, exist_ok=True)
    os.makedirs(SUMMARY_PATH, exist_ok=True)
    
    # 3. 분석 대상 파일 리스트 확보
    files = glob.glob(os.path.join(tester.minute_path, "*.parquet"))
    all_tickers = [os.path.basename(f).split('.')[0] for f in files]
    
    total_count = len(all_tickers)
    start_time_all = time.time()
    
    print(f"🚀 총 {total_count}개 종목 백테스트 시작: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"📂 상세 경로: {TRADES_PATH}")
    print(f"📊 요약 경로: {SUMMARY_PATH}")
    print("=" * 60)
    
    summary_list = []
    
    # 4. 루프 실행
    for i, ticker in enumerate(all_tickers):
        ticker_start_time = time.time()
        current_num = i + 1
        
        try:
            # 백테스트 실행
            pf = tester.run_backtest(ticker)
            
            if pf is not None:
                # A. 요약 통계(Stats) 추출
                stats = pf.stats()
                stats['Ticker'] = ticker
                summary_list.append(stats)
                
                # B. 상세 거래 내역(Trades) 저장
                if not pf.trades.records.empty:
                    # 1. 원본 레코드 데이터 추출 (훨씬 빠름)
                    trades_df = pf.trades.records
                    
                    # 2. 인덱스 번호를 실제 날짜로 변환 (속도 최적화 방식)
                    # vbt.Portfolio 객체(pf)의 인덱스를 사용하여 날짜 매핑
                    idx_to_date = pf.wrapper.index
                    trades_df['entry_date'] = idx_to_date[trades_df['entry_idx']]
                    trades_df['exit_date'] = idx_to_date[trades_df['exit_idx']]
                    
                    # 3. 저장
                    trade_file = os.path.join(TRADES_PATH, f"trades_{ticker}.parquet")
                    trades_df.to_parquet(trade_file, engine='fastparquet', index=True)
                                
                # C. 메모리 해제
                del pf
                
            # 진행 상황 출력 (10종목마다 상세 출력, 매 종목마다 간략 출력)
            elapsed_ticker = time.time() - ticker_start_time
            progress_pct = (current_num / total_count) * 100
            
            print(f"[{current_num:4d}/{total_count:4d}] {progress_pct:6.2f}% | {ticker:8s} | {elapsed_ticker:5.2f}s 완료", end='\r')
            
            if current_num % 50 == 0:
                print(f"\n📢 중간 점검: {current_num}개 완료 (누적 소요시간: {(time.time() - start_time_all)/60:.1f}분)")

        except Exception as e:
            print(f"\n❌ {ticker} 에러 발생: {e}")
            continue

    # 5. 최종 리포트 저장
    print("\n" + "=" * 60)
    if summary_list:
        final_summary_df = pd.DataFrame(summary_list)
        
        # 수익률 기준 정렬
        if 'Total Return [%]' in final_summary_df.columns:
            final_summary_df.sort_values(by='Total Return [%]', ascending=False, inplace=True)
        
        report_file = os.path.join(SUMMARY_PATH, "total_backtest_report.csv")
        final_summary_df.to_csv(report_file, index=False, encoding='utf-8-sig')
        
        total_elapsed = (time.time() - start_time_all) / 60
        print(f"✅ 전체 분석 완료! (총 소요시간: {total_elapsed:.1f}분)")
        print(f"📊 최종 리포트: {report_file}")
    else:
        print("⚠️ 생성된 결과가 없습니다.")

if __name__ == "__main__":
    run_mass_backtest()