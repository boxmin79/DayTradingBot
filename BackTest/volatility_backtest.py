import pandas as pd
import numpy as np
import os

class VolatilityBacktest:
    def __init__(self, strategy, base_dir="data/backtest/result"):
        """
        백테스트 엔진 초기화
        :param strategy: 실행할 전략 객체 (예: VolatilityBreakout)
        :param base_dir: 결과물이 저장될 기본 루트 디렉토리
        """
        self.strategy = strategy
        # 전략 클래스명을 소문자로 가져와 하위 폴더 및 파일명에 사용
        self.strategy_name = self.strategy.__class__.__name__.lower()
        self.base_dir = base_dir
        self.output_dir = os.path.join(base_dir, self.strategy_name)
        
        # 전체 종목의 성적을 모아둘 리스트 (최종 요약 CSV용)
        self.total_summary_logs = []
        # 최근 매매 결과 저장 (최근 승률 등 상태 지표 계산용)
        self.recent_results = []

        # 결과 저장 폴더 생성
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir, exist_ok=True)

    def run(self, ticker, minute_df, daily_df):
        """
        특정 종목에 대한 백테스트 수행
        """
        try:
            # 1. 데이터 전처리 및 병합 (분봉 + 일봉 지표)
            df = self._prepare_data(minute_df, daily_df)

            # 2. 전략 적용 (목표가 계산 및 기술적 지표 추가)
            # 전략 내부에서 IndicatorFactory.add_all_indicators(df)를 호출한다고 가정
            df = self.strategy.apply_strategy(df)

            # 3. 매매 시뮬레이션 및 스냅샷 채득
            ticker_trade_logs = self._simulate(ticker, df)

            # 4. 결과 저장
            if ticker_trade_logs:
                # (1) 종목별 상세 상관관계 분석 파일 저장
                analysis_df = pd.DataFrame(ticker_trade_logs)
                analysis_df.to_csv(os.path.join(self.output_dir, f"{ticker}_analysis.csv"), index=False)
                
                # (2) 종목별 요약 통계 계산 후 마스터 리스트에 추가
                self._add_to_summary(ticker, analysis_df)
                print(f"[{ticker}] 백테스트 완료: {len(ticker_trade_logs)}회 거래 발생")
            else:
                print(f"[{ticker}] 조건 충족 거래 없음")
                
        except Exception as e:
            print(f"[{ticker}] 백테스트 중 오류 발생: {e}")

    def _prepare_data(self, minute_df, daily_df):
        """분봉 데이터에 전일 고가, 저가, 종가 및 일봉 지표 병합"""
        daily_df = daily_df.copy()
        daily_df['prev_high'] = daily_df['high'].shift(1)
        daily_df['prev_low'] = daily_df['low'].shift(1)
        daily_df['prev_close'] = daily_df['close'].shift(1)
        
        # 일봉 기반 변동성(ATR) 및 이평선 (상태 지표용)
        daily_df['daily_atr'] = (daily_df['high'] - daily_df['low']).rolling(14).mean()
        
        # 날짜 기준으로 병합
        combined = pd.merge(minute_df, 
                            daily_df[['date', 'prev_high', 'prev_low', 'prev_close', 'daily_atr', 'ma120', 'ma60']], 
                            on='date', how='left')
        return combined

    def _simulate(self, ticker, df):
        """거래 발생 시점의 지표들을 나열하여 추출"""
        ticker_logs = []
        grouped = df.groupby('date')
        
        for date, day_df in grouped:
            if day_df.empty: continue
            
            # 전략에서 계산된 값들 가져오기
            target_price = day_df['target_price'].iloc[0]
            day_open = day_df['open'].iloc[0]
            prev_close = day_df['prev_close'].iloc[0]
            
            # 상태 지표: 시가 갭(%)
            gap_ratio = (day_open - prev_close) / prev_close if prev_close else 0

            # 돌파 감지 (당일 고가가 목표가 이상인 경우)
            breakout = day_df[day_df['high'] >= target_price]
            
            if not breakout.empty:
                entry_row = breakout.iloc[0]
                entry_price = target_price 
                exit_price = day_df['close'].iloc[-1] # 종가 청산
                profit_ratio = (exit_price / entry_price) - 1

                # 최근 10회 승률 (현재까지의 매매 심리 상태 지표)
                recent_win_rate = sum(1 for r in self.recent_results[-10:] if r > 0) / 10 if self.recent_results else 0
                
                # [상관관계 분석용 스냅샷] 지표 나열
                snapshot = {
                    'ticker': ticker,
                    'date': date,
                    'entry_time': entry_row.name,
                    'profit': profit_ratio,
                    'rsi': entry_row.get('rsi'),
                    'vol_ratio': entry_row.get('volume_ratio'),
                    'gap_ratio': gap_ratio,
                    'recent_win_rate': recent_win_rate,
                    'ma120_dist': entry_row['close'] / entry_row['ma120'] if entry_row['ma120'] else 1,
                    'atr_status': entry_row['daily_atr'] / entry_price if entry_price else 0
                }
                
                ticker_logs.append(snapshot)
                self.recent_results.append(profit_ratio)
                
        return ticker_logs

    def _add_to_summary(self, ticker, df):
        """종목별 최종 성적표 작성 (수익성, 안정성, 효율성)"""
        profits = df['profit']
        
        # 수익성 지표
        total_return = (1 + profits).prod() - 1
        win_rate = (profits > 0).mean() * 100
        pf = profits[profits > 0].sum() / abs(profits[profits < 0].sum()) if any(profits < 0) else 999
        
        # 안정성 지표 (MDD)
        equity_curve = (1 + profits).cumprod()
        drawdown = (equity_curve.cummax() - equity_curve) / equity_curve.cummax()
        mdd = drawdown.max() * 100
        
        # 효율성 지표 (Sharpe - 단순 계산)
        sharpe = (profits.mean() / profits.std() * np.sqrt(252)) if profits.std() != 0 else 0

        summary = {
            'ticker': ticker,
            'total_return_pct': total_return * 100,
            'win_rate': win_rate,
            'profit_factor': pf,
            'mdd_pct': mdd,
            'sharpe_ratio': sharpe,
            'trade_count': len(df)
        }
        self.total_summary_logs.append(summary)

    def save_final_summary(self):
        """모든 종목의 테스트가 끝난 후 통합 요약 보고서 CSV 저장"""
        if not self.total_summary_logs:
            print("저장할 요약 데이터가 없습니다.")
            return

        summary_df = pd.DataFrame(self.total_summary_logs)
        file_path = os.path.join(self.base_dir, f"{self.strategy_name}_summary.csv")
        summary_df.to_csv(file_path, index=False)
        print(f"\n[!] 통합 요약 보고서 저장 완료: {file_path}")