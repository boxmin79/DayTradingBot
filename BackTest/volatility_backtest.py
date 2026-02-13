import os
import sys
import pandas as pd
from Collector.add_indicator import ChartIndicatorAdder 

class VolatilityBacktest:
    def __init__(self, strategy, base_dir="data/backtest/result"):
        self.strategy = strategy
        self.strategy_name = self.strategy.name
        self.base_dir = base_dir
        self.output_dir = os.path.join(base_dir, self.strategy_name)
        self.total_summary_logs = []
        self.indicator_adder = ChartIndicatorAdder()
        
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir, exist_ok=True)

    def run(self, ticker, minute_df, daily_df, save=False):
        try:
            # 1. 지표 추가
            minute_df = self.indicator_adder.add_indicators(minute_df)
            daily_df = self.indicator_adder.add_indicators(daily_df)
            
            if minute_df.empty or daily_df.empty:
                return False

            # 2. 목표가 계산
            daily_df['range'] = daily_df['high'].shift(1) - daily_df['low'].shift(1)
            daily_df['target_price'] = daily_df['open'] + (daily_df['range'] * self.strategy.k)
            
            valid_daily = daily_df.dropna(subset=['target_price'])

            # 3. 고속 시뮬레이션 실행 (필터 로직 포함)
            ticker_trade_logs = self._simulate_efficient(ticker, minute_df, valid_daily)

            if ticker_trade_logs:
                self._add_to_summary(ticker, ticker_trade_logs)
                if save:
                    res_df = pd.DataFrame(ticker_trade_logs)
                    save_path = os.path.join(self.output_dir, f"{ticker}.parquet")
                    res_df.to_parquet(save_path, engine='fastparquet', compression='snappy', index=False)
                return True
                
        except Exception as e:
            print(f"[{ticker}] 백테스트 중 에러 발생: {e}")
        return False

    def _simulate_efficient(self, ticker, min_df, day_df):
        logs = []
        day_dict = day_df.set_index('date').to_dict('index')
        
        total_cost_ratio = 0.003 
        stop_loss_rate = getattr(self.strategy, 'stop_loss_rate', 0.02)

        target_dict = {d: v['target_price'] for d, v in day_dict.items()}
        close_dict = {d: v['close'] for d, v in day_dict.items()}

        min_df['target_price'] = min_df['date'].map(target_dict)
        active_df = min_df.dropna(subset=['target_price']).copy()
        
        active_df['is_breakout'] = active_df['high'] >= active_df['target_price']
        breakout_days = active_df[active_df['is_breakout']].groupby('date')

        for date, entries in breakout_days:
            # --- [필터링 로직 추가 시작] ---
            # 당일 일봉 지표 데이터 추출
            day_data = day_dict.get(date)
            if not day_data: continue

            # 필터 1: 상승 가속도 (MACD 히스토그램 기울기) - 수익 거래 평균 99 vs 손실 거래 25
            if day_data.get('macd_hist_slope', 0) < 30: continue

            # 필터 2: 일봉 몸통 비율 (양봉의 견고함) - 수익 거래 0.70 vs 손실 거래 0.34
            if day_data.get('body_ratio', 0) < 0.5: continue

            # 필터 3: 가격 위치 (볼린저 밴드 상단 근처) - 수익 거래 0.68 vs 손실 거래 0.52
            if day_data.get('band_p', 0) < 0.6: continue

            # 필터 4: 과도한 시가 갭 상승 제한 (리스크 관리)
            if day_data.get('gap_ratio', 0) > 0.03: continue
            # --- [필터링 로직 추가 종료] ---

            entry_row = entries.iloc[0] 
            buy_price = entry_row['target_price']
            stop_price = buy_price * (1 - stop_loss_rate)
            sell_price = close_dict.get(date)
            is_stop_loss = False

            day_min_full = min_df[min_df['date'] == date]
            if day_min_full['low'].min() <= stop_price:
                sell_price = stop_price
                is_stop_loss = True

            if sell_price:
                profit_rate = (sell_price / buy_price) - 1 - total_cost_ratio
                
                trade_data = {
                    'ticker': ticker, 'date': date, 'profit': profit_rate,
                    'buy_price': buy_price, 'sell_price': sell_price,
                    'is_stop_loss': is_stop_loss, 'entry_time': entry_row['time']
                }
                
                # 지표 기록 (분석용)
                min_row_dict = entry_row.to_dict()
                for k, v in min_row_dict.items():
                    if k not in ['ticker', 'date', 'time', 'target_price']:
                        trade_data[f"m_{k}"] = v
                
                for k, v in day_data.items():
                    if k not in ['target_price', 'close', 'open', 'high', 'low', 'volume']:
                        trade_data[f"d_{k}"] = v
                
                logs.append(trade_data)
                
        return logs

    def _add_to_summary(self, ticker, trade_logs):
        df = pd.DataFrame(trade_logs)
        profits = df['profit']
        self.total_summary_logs.append({
            'ticker': ticker,
            'total_return': ((1 + profits).prod() - 1) * 100,
            'win_rate': (profits > 0).mean() * 100,
            'trade_count': len(df),
            'avg_profit': profits.mean() * 100
        })