import pandas as pd
from Strategy.volatility_breakout import VolatilityBreakout
from BackTest.volatility_backtest import VolatilityBacktest
from multiprocessing import Pool, cpu_count
import os
import time
import sys
from datetime import timedelta

# [수정] 병렬 프로세스 작업 함수 (반환값 개선)
def run_backtest_process(ticker):
    try:
        daily_file = f"data/chart/daily/{ticker}.parquet"
        minute_file = f"data/chart/minute/{ticker}.parquet"
        
        if not os.path.exists(daily_file) or not os.path.exists(minute_file):
            return None

        # 2. 데이터 로드
        try:
            daily_df = pd.read_parquet(daily_file)
            minute_df = pd.read_parquet(minute_file)
        except Exception:
            return None
        
        # 3. [수정] 'date' 컬럼명 통일 (datetime -> date 변환 추가)
        
        # (1) 일봉 데이터 검증 및 변환
        if 'date' not in daily_df.columns:
            if 'datetime' in daily_df.columns:  # <--- [추가됨] 이 부분이 핵심!
                daily_df = daily_df.rename(columns={'datetime': 'date'})
            elif 'Date' in daily_df.columns:
                daily_df = daily_df.rename(columns={'Date': 'date'})
            elif '일자' in daily_df.columns:
                daily_df = daily_df.rename(columns={'일자': 'date'})
            elif daily_df.index.name == 'date':
                daily_df = daily_df.reset_index()
            else:
                return None # 날짜 컬럼이 없으면 스킵

        # [수정] 날짜 포맷 통일 (YYYY-MM-DD 문자열 -> YYYYMMDD 정수)
        # 분봉 데이터(int64)와 병합하기 위해 일봉 데이터의 타입을 맞춤
        if daily_df['date'].dtype == 'object':
            daily_df['date'] = pd.to_datetime(daily_df['date']).dt.strftime('%Y%m%d').astype(int)

        # (2) 분봉 데이터 검증 및 변환
        if 'date' not in minute_df.columns:
            if 'datetime' in minute_df.columns: # <--- [추가됨] 분봉도 동일하게 처리
                minute_df = minute_df.rename(columns={'datetime': 'date'})
            elif 'Date' in minute_df.columns:
                minute_df = minute_df.rename(columns={'Date': 'date'})
            elif '일자' in minute_df.columns:
                minute_df = minute_df.rename(columns={'일자': 'date'})
            elif minute_df.index.name == 'date':
                minute_df = minute_df.reset_index()
            else:
                return None
        
        strategy = VolatilityBreakout(k=0.5)
        backtester = VolatilityBacktest(strategy)
        
        is_traded = backtester.run(ticker, minute_df, daily_df)
        
        # [핵심] 거래가 발생했다면 요약 데이터(Dict)를 반환 (취합용)
        if is_traded and backtester.total_summary_logs:
            return backtester.total_summary_logs[-1]
            
        return None
        
    except Exception:
        return None

if __name__ == '__main__':
    # 1. 대상 종목 리스트 로드
    data_dir = "data/chart/daily"
    # 파일명에서 확장자 제거하여 티커 리스트 생성
    tickers = [f.replace('.parquet', '') for f in os.listdir(data_dir) if f.endswith('.parquet')]
    
    total_count = len(tickers)
    print(f"\n[System] 총 {total_count}개 종목 백테스트 시작...")
    print(f"[System] 사용 CPU 코어: {cpu_count()}개\n")
    
    start_time = time.time()
    
    # 결과 취합용 리스트
    final_summaries = []
    success_count = 0

    # 2. 멀티 프로세싱 (진행상황 시각화 추가)
    with Pool(cpu_count()) as pool:
        # imap_unordered: 작업이 끝나는 순서대로 즉시 결과를 내뱉음 (실시간 갱신용)
        for i, result in enumerate(pool.imap_unordered(run_backtest_process, tickers), 1):
            
            # 거래가 발생한 경우 결과 저장
            if result is not None:
                final_summaries.append(result)
                success_count += 1
            
            # --- [시각화 로직] ---
            # 진행률 계산
            progress = i / total_count
            percent = progress * 100
            
            # 경과 시간 포맷팅 (00:00:12)
            elapsed = time.time() - start_time
            elapsed_str = str(timedelta(seconds=int(elapsed)))
            
            # 남은 시간 추정 (ETA)
            if i > 10: # 초반엔 오차가 크므로 10개 이후부터 계산
                avg_time = elapsed / i
                remain_time = avg_time * (total_count - i)
                eta_str = str(timedelta(seconds=int(remain_time)))
            else:
                eta_str = "계산중..."

            # 진행 바 그리기 ( [#####-----] )
            bar_length = 30
            filled_length = int(bar_length * progress)
            bar = '█' * filled_length + '-' * (bar_length - filled_length)
            
            # 한 줄에 덮어쓰기 출력 (\r)
            sys.stdout.write(f"\r[{bar}] {percent:5.1f}% ({i}/{total_count}) | 경과: {elapsed_str} | 남은시간: {eta_str} | 거래발생: {success_count}건")
            sys.stdout.flush()

    # 3. 최종 결과 저장 (취합된 요약본)
    print("\n\n[System] 백테스트 완료. 결과 집계 중...")
    
    if final_summaries:
        summary_df = pd.DataFrame(final_summaries)
        # 결과 저장 경로 (Strategy 이름 폴더)
        strategy_name = VolatilityBreakout().__class__.__name__.lower()
        save_path = f"data/backtest/result/{strategy_name}_summary.csv"
        
        summary_df.to_csv(save_path, index=False)
        print(f"[Save] 통합 요약본 저장 완료: {save_path}")
        
        # 간단한 리포트 출력
        avg_return = summary_df['total_return'].mean()
        avg_win_rate = summary_df['win_rate'].mean()
        print(f"\n📊 [최종 요약]")
        print(f"- 평균 수익률: {avg_return:.2f}%")
        print(f"- 평균 승률: {avg_win_rate:.2f}%")
        print(f"- 거래된 종목: {success_count} / {total_count}개")
        
    else:
        print("[Alert] 거래된 종목이 하나도 없습니다.")

    print(f"\n총 소요 시간: {str(timedelta(seconds=int(time.time() - start_time)))}")