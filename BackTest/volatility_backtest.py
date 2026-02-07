import pandas as pd
import numpy as np
from .backtest import Backtest

class VolatilityBacktest(Backtest):
    def __init__(self, strategy):
        super().__init__(strategy)

    def run(self, df: pd.DataFrame, ticker=None, fee=0.0015): 
        if not isinstance(df.index, pd.DatetimeIndex):
            try:
                dt_str = df['date'].astype(str) + df['time'].astype(str).str.zfill(4)
                df.index = pd.to_datetime(dt_str, format='%Y%m%d%H%M')
            except: return pd.DataFrame(), None

        df = self.strategy.apply_strategy(df)
        daily_records = []
        holding_times = []
        total_commissions = 0 # 총 지불 비용 합계

        grouped = df.groupby(df.index.date)
        for date, day_df in grouped:
            target_price = day_df['target_price'].iloc[0]
            if pd.isna(target_price) or target_price <= 0: continue

            breakout = day_df[day_df['high'] >= target_price]
            if not breakout.empty:
                first_candle = breakout.iloc[0]
                entry_time = breakout.index[0]
                exit_time = day_df.index[-1]
                holding_times.append((exit_time - entry_time).seconds / 60)

                real_buy_price = max(target_price, first_candle['open'])
                sell_price = day_df['close'].iloc[-1]
                
                # 비용 분석
                comm_cost = fee * 2 # 왕복 수수료+세금
                total_commissions += comm_cost
                
                real_ret = (sell_price - real_buy_price) / real_buy_price - comm_cost
                theo_ret = (sell_price - target_price) / target_price - comm_cost
                slip_cost = theo_ret - real_ret
            else:
                real_ret = slip_cost = 0.0
                
            daily_records.append({'date': date, 'return': real_ret, 'slippage_cost': slip_cost})
            
        result_df = pd.DataFrame(daily_records)
        if result_df.empty: return pd.DataFrame(), None
        result_df.set_index('date', inplace=True)
        
        metrics = self.calculate_metrics(result_df)
        if metrics:
            metrics['평균보유시간(분)'] = f"{np.mean(holding_times):.1f}" if holding_times else "0"
            metrics['누적비용(비중)'] = f"{total_commissions:.2%}" # 전체 매매 횟수 대비 누적 비용
            metrics['총슬리피지'] = f"{result_df['slippage_cost'].sum():.2%}"
        
        if ticker:
            self.save_to_csv(result_df, ticker, metrics=metrics)
        return result_df, metrics