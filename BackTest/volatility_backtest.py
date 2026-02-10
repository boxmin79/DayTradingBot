import pandas as pd
import numpy as np
import os

class VolatilityBacktest:
    def __init__(self, strategy, base_dir="data/backtest/result"):
        self.strategy = strategy
        self.strategy_name = self.strategy.__class__.__name__.lower()
        self.base_dir = base_dir
        self.output_dir = os.path.join(base_dir, self.strategy_name)
        
        self.total_summary_logs = []  # summary.csv용
        self.recent_results = []      # 심리 지표용 (최근 10회 승률)
        
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir, exist_ok=True)

    def run(self, ticker, minute_df, daily_df):
        try:
            # 1. 일봉 지표 확장 및 결합 (Context)
            df = self._prepare_data(minute_df, daily_df)

            # 2. 전략 적용 (목표가 계산 및 분봉 지표 생성)
            # Strategy 클래스 내부에서 ma5, ma20, bb_upper 등의 분봉 지표가 생성되어야 함
            df = self.strategy.apply_strategy(df)

            # 3. 매매 시뮬레이션 및 초정밀 스냅샷 채득
            ticker_trade_logs = self._simulate(ticker, df)

            # 4. 결과 저장
            if ticker_trade_logs:
                analysis_df = pd.DataFrame(ticker_trade_logs)
                analysis_df.to_csv(os.path.join(self.output_dir, f"{ticker}_analysis.csv"), index=False)
                self._add_to_summary(ticker, analysis_df)
                return True
        except Exception as e:
            print(f"[{ticker}] 백테스트 중 오류 발생: {e}")
        return False

    def _prepare_data(self, minute_df, daily_df):
        """일봉의 거시적 데이터(추세, 수급)를 분봉에 결합"""
        d = daily_df.copy()
        
        # 기본 가격 데이터 (전일 기준)
        d['prev_high'] = d['high'].shift(1)
        d['prev_low'] = d['low'].shift(1)
        d['prev_close'] = d['close'].shift(1)
        d['prev_vol'] = d['volume'].shift(1)
        
        # 일봉 이동평균선 (추세 필터용)
        d['d_ma5'] = d['close'].rolling(5).mean()
        d['d_ma20'] = d['close'].rolling(20).mean()
        d['d_ma60'] = d['close'].rolling(60).mean()
        d['d_ma120'] = d['close'].rolling(120).mean()
        
        # 일봉 변동성 및 거래량 이동평균
        d['d_atr'] = (d['high'] - d['low']).rolling(14).mean()
        d['d_vol_ma20'] = d['volume'].rolling(20).mean().shift(1) 
        
        # 분봉 데이터와 병합할 컬럼 선별
        cols = ['date', 'prev_high', 'prev_low', 'prev_close', 'prev_vol', 
                'd_ma5', 'd_ma20', 'd_ma60', 'd_ma120', 'd_atr', 'd_vol_ma20']
        combined = pd.merge(minute_df, d[cols], on='date', how='left')
        return combined

    def _simulate(self, ticker, df):
        """거래 발생 시점의 분봉(미시) + 일봉(거시) 지표를 모두 기록"""
        ticker_logs = []
        grouped = df.groupby('date')
        
        for date, day_df in grouped:
            if day_df.empty: continue
            
            # 기준 데이터 추출
            target_price = day_df['target_price'].iloc[0]
            day_open = day_df['open'].iloc[0]
            prev_close = day_df['prev_close'].iloc[0]
            
            # [일봉] 시가 대비 목표가 거리 (너무 멀면 진입 자제)
            open_to_target = (target_price / day_open) - 1 if day_open else 0

            # 돌파 감지 (고가가 목표가 이상)
            breakout = day_df[day_df['high'] >= target_price]
            
            if not breakout.empty:
                entry_row = breakout.iloc[0] # 돌파가 일어난 첫 분봉
                entry_price = target_price 
                exit_price = day_df['close'].iloc[-1] # 종가 청산
                profit_ratio = (exit_price / entry_price) - 1

                # [성과] 당일 최대 상승폭 (익절 포텐셜)
                day_high = day_df['high'].max()
                max_potential = (day_high / entry_price) - 1

                # [심리] 최근 10회 승률
                recent_wr = sum(1 for r in self.recent_results[-10:] if r > 0) / 10 if self.recent_results else 0
                
                # --- 스냅샷 작성 (분석의 핵심) ---
                snapshot = {
                    'ticker': ticker,
                    'date': date,
                    'entry_time': entry_row['time'], # 시간 (index가 시간인 경우) 혹은 entry_row['time']
                    'day_of_week': pd.to_datetime(str(date)).dayofweek, # 0:월 ~ 4:금
                    
                    'profit': profit_ratio,
                    'max_potential': max_potential,
                    
                    # 1. 분봉 기술적 지표 (Trigger Detail) - 여기가 대폭 보강됨
                    'm_rsi': entry_row.get('rsi', np.nan),
                    'm_vol_ratio': entry_row.get('volume_ratio', np.nan),
                    'm_macd_hist': entry_row.get('macd_hist', np.nan),
                    
                    # 분봉 이평선 이격도 (단기 과열 판단)
                    'm_ma5_dist': entry_row['close'] / entry_row['ma5'] if 'ma5' in entry_row and pd.notna(entry_row['ma5']) and entry_row['ma5'] != 0 else 1,
                    'm_ma20_dist': entry_row['close'] / entry_row['ma20'] if 'ma20' in entry_row and pd.notna(entry_row['ma20']) and entry_row['ma20'] != 0 else 1,
                    
                    # 분봉 볼린저밴드 위치 (상단 돌파 강도)
                    'm_bb_upper_dist': entry_row['close'] / entry_row['bb_upper'] if 'bb_upper' in entry_row and pd.notna(entry_row['bb_upper']) and entry_row['bb_upper'] != 0 else 1,
                    
                    # 분봉 캔들 모양 (꼬리, 몸통)
                    'm_candle_body': (entry_row['close'] - entry_row['open']) / entry_row['open'] if pd.notna(entry_row['open']) and entry_row['open'] != 0 else 0,
                    'm_high_tail': (entry_row['high'] - entry_row['close']) / entry_row['close'] if pd.notna(entry_row['close']) and entry_row['close'] != 0 else 0,
                    
                    # 2. 일봉 추세 지표 (Context Filter)
                    'd_gap_ratio': (day_open - prev_close) / prev_close if pd.notna(prev_close) and prev_close != 0 else 0,
                    'd_open_to_target': open_to_target,
                    'd_ma5_dist': entry_row['close'] / entry_row['d_ma5'] if pd.notna(entry_row['d_ma5']) and entry_row['d_ma5'] != 0 else 1,
                    'd_ma60_dist': entry_row['close'] / entry_row['d_ma60'] if pd.notna(entry_row['d_ma60']) and entry_row['d_ma60'] != 0 else 1,
                    'd_trend_short': 1 if pd.notna(entry_row['d_ma5']) and pd.notna(entry_row['d_ma20']) and entry_row['d_ma5'] > entry_row['d_ma20'] else 0,
                    'd_trend_long': 1 if pd.notna(entry_row['d_ma20']) and pd.notna(entry_row['d_ma120']) and entry_row['d_ma20'] > entry_row['d_ma120'] else 0,
                    
                    # 3. 수급 에너지
                    'd_vol_status': day_df['volume'].cumsum().loc[entry_row.name] / entry_row['prev_vol'] if pd.notna(entry_row['prev_vol']) and entry_row['prev_vol'] != 0 else 0,
                    'd_avg_vol_ratio': day_df['volume'].cumsum().loc[entry_row.name] / entry_row['d_vol_ma20'] if pd.notna(entry_row['d_vol_ma20']) and entry_row['d_vol_ma20'] != 0 else 0,
                    
                    'recent_wr': recent_wr
                }
                
                ticker_logs.append(snapshot)
                self.recent_results.append(profit_ratio)
                
        return ticker_logs

    def _add_to_summary(self, ticker, df):
        p = df['profit']
        equity = (1 + p).cumprod()
        
        summary = {
            'ticker': ticker,
            'total_return': (equity.iloc[-1] - 1) * 100,
            'win_rate': (p > 0).mean() * 100,
            'profit_factor': p[p > 0].sum() / abs(p[p < 0].sum()) if any(p < 0) else 99,
            'mdd': ((equity.cummax() - equity) / equity.cummax()).max() * 100,
            'trade_count': len(df)
        }
        self.total_summary_logs.append(summary)

    def save_final_summary(self):
        if self.total_summary_logs:
            pd.DataFrame(self.total_summary_logs).to_csv(
                os.path.join(self.base_dir, f"{self.strategy_name}_summary.csv"), index=False
            )
            print(f"\n[!] 1,900개 종목 분석 통합 보고서 저장 완료.")