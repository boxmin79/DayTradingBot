import pandas as pd
from .backtest import Backtest

class VolatilityBacktest(Backtest):
    """
    변동성 돌파 전용 실행기:
    부모 엔진의 기능을 활용하여 실제 진입/청산 시뮬레이션을 수행합니다.
    """
    def __init__(self, strategy):
        super().__init__(strategy)

    def run(self, df: pd.DataFrame, ticker=None, fee=0.0015): 
        # 1. 시계열 데이터 전처리
        if not isinstance(df.index, pd.DatetimeIndex):
            dt_str = df['date'].astype(str) + df['time'].astype(str).str.zfill(4)
            df.index = pd.to_datetime(dt_str, format='%Y%m%d%H%M')

        # 2. 전략 적용 (목표가 계산)
        df = self.strategy.apply_strategy(df)
        daily_records = []
        
        # 3. 매매 시뮬레이션 로직
        grouped = df.groupby(df.index.date)
        for date, day_df in grouped:
            target_price = day_df['target_price'].iloc[0]
            if pd.isna(target_price) or target_price <= 0: continue

            # 돌파 여부 확인
            breakout = day_df[day_df['high'] >= target_price]
            if not breakout.empty:
                first_candle = breakout.iloc[0]
                entry_time = breakout.index[0]
                
                # 진입가/청산가 결정 (슬리피지 및 수수료 반영)
                real_buy_price = max(target_price, first_candle['open'])
                sell_price = day_df['close'].iloc[-1]
                
                real_ret = (sell_price - real_buy_price) / real_buy_price - (fee * 2)
                
                daily_records.append({
                    'date': date,
                    'entry_time': entry_time,
                    'target_price': target_price,
                    'entry_price': real_buy_price,
                    'exit_price': sell_price,
                    'return': real_ret
                })
            
        result_df = pd.DataFrame(daily_records)
        if result_df.empty: return pd.DataFrame(), None
        result_df.set_index('date', inplace=True)
        
        # 4. 공통 엔진을 통한 지표 산출 및 저장
        metrics = self.calculate_metrics(result_df)
        if metrics and ticker:
            self.save_results(result_df, metrics, ticker)
                
        return result_df, metrics