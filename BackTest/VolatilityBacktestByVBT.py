import vectorbt as vbt
import pandas as pd
import os
import numpy as np

class VolatilityBacktester:
    def __init__(self, slippage=0.001, fees=0.00015, tax=0.0015, stop_loss=0.02, k=0.6):
        self.daily_path = os.path.join("data", "chart", "daily")
        self.minute_path = os.path.join("data", "chart", "minute")
        self.k = k
        self.fees = fees
        self.tax = tax
        self.stop_loss = stop_loss
        self.slippage = slippage

    def _load_data(self, ticker):
        d_path = os.path.join(self.daily_path, f"{ticker}.parquet")
        m_path = os.path.join(self.minute_path, f"{ticker}.parquet")
        
        if not os.path.exists(m_path):
            return None, None
            
        try:
            m_df = pd.read_parquet(m_path, engine='fastparquet')
            if m_df.empty: return None, None
            
            if 'date' in m_df.columns:
                m_df.set_index('date', inplace=True)
            m_df.index = pd.to_datetime(m_df.index)
            
            if os.path.exists(d_path):
                d_df = pd.read_parquet(d_path, engine='fastparquet')
                if 'date' in d_df.columns:
                    d_df.set_index('date', inplace=True)
                d_df.index = pd.to_datetime(d_df.index)
            else:
                d_df = m_df.resample('D').agg({
                    'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last', 'volume': 'sum'
                }).dropna().sort_index()
            
            for df in [d_df, m_df]:
                for col in ['open', 'high', 'low', 'close', 'volume']:
                    if col in df.columns:
                        df[col] = df[col].astype('float64')
            
            return d_df, m_df
        except Exception:
            return None, None

    def run_backtest(self, ticker, value_limit=10_000_000_000):
        try:
            d_df, m_df = self._load_data(ticker)
            if d_df is None or m_df is None or d_df.empty or m_df.empty:
                return None
            
            # --- 1. 기본 지표 계산 ---
            d_df['trading_value'] = d_df['close'] * d_df['volume']
            d_df['avg_value_5d'] = d_df['trading_value'].rolling(window=5).mean()
            ma5 = d_df['close'].rolling(window=5).mean()
            
            # --- 2. [신규] 5MA 기울기 및 이격도 필터 (분석 결과 반영) ---
            # 기울기 (Slope): (현재MA5 / 이전MA5 - 1) * 100
            d_df['slope5'] = ((ma5 / ma5.shift(1)) - 1) * 100
            # 이격도 (Disparity): (현재가 / MA5 - 1) * 100
            d_df['disp5'] = ((d_df['close'] / ma5) - 1) * 100

            # [통계 기반 필터 조건]
            # 1. 승률 38% 최적 구간 (공격적 필터)
            # is_opt_zone = (d_df['slope5'] > 1.3) & (d_df['slope5'] <= 1.74) & (d_df['disp5'] > 2.7) & (d_df['disp5'] <= 3.6)
            
            # 2. 범용 승률 우위 구간 (기울기 양수 및 적정 이격도)
            # 분석 결과 Win 평균인 slope5 > 1.0% 및 disp5 > 2.6% 근처를 기준으로 설정
            is_slope_good = d_df['slope5'].shift(1) > 0.8  # 전일 기준 기울기가 탄탄한가
            is_disp_good = d_df['disp5'].shift(1) > 1.0    # 전일 기준 정배열 탄력이 붙었는가
            
            # --- 3. 기존 필터 유지 ---
            is_liquid = d_df['avg_value_5d'].shift(1) >= value_limit
            is_trend_up = d_df['close'].shift(1) > ma5.shift(1)
            avg_vol_5d = d_df['volume'].rolling(window=5).mean()
            
            prev_range = (d_df['high'] - d_df['low']).shift(1)
            target_price = d_df['open'] + prev_range * self.k
            
            # --- 4. 데이터 매핑 ---
            m_df['date'] = m_df.index.normalize()
            m_df['target_price'] = m_df['date'].map(target_price)
            m_df['is_trend_up'] = m_df['date'].map(is_trend_up)
            m_df['is_liquid'] = m_df['date'].map(is_liquid)
            
            # 신규 필터 매핑
            m_df['is_slope_good'] = m_df['date'].map(is_slope_good)
            m_df['is_disp_good'] = m_df['date'].map(is_disp_good)
            
            m_df['ref_vol'] = m_df['date'].map(avg_vol_5d.shift(1))
            m_df['cum_vol'] = m_df.groupby('date')['volume'].cumsum()
            
            # --- 5. 신호 생성 (통계 필터 통합) ---
            condition = (
                (m_df['high'] >= m_df['target_price']) & 
                (m_df['is_trend_up'] == True) & 
                (m_df['is_liquid'] == True) & 
                (m_df['is_slope_good'] == True) & # 기울기 필터
                (m_df['is_disp_good'] == True) &  # 이격도 필터
                (m_df['cum_vol'] > m_df['ref_vol'] * 0.5)
            )
            
            entries = condition.groupby(m_df['date']).transform(lambda x: x & (x.cumsum() == 1))
            exits = (m_df.index.hour == 15) & (m_df.index.minute == 19)
            exits = exits | (m_df.groupby('date').cumcount(ascending=False) == 0)

            if entries.sum() == 0: return None

            # --- 6. 시뮬레이션 ---
            actual_entry = m_df['open'].where(m_df['open'] > m_df['target_price'], m_df['target_price'])
            exec_price = m_df['close'].copy() 
            exec_price.loc[entries] = actual_entry.loc[entries]

            pf = vbt.Portfolio.from_signals(
                close=m_df['close'], entries=entries, exits=exits, price=exec_price,
                fees=self.fees + (self.tax / 2), slippage=self.slippage,
                high=m_df['high'], low=m_df['low'],
                sl_stop=self.stop_loss, sl_trail=True,
                tp_stop=0.07,
                accumulate=False, freq='1min'
            )
            return pf
        except Exception:
            return None

if __name__ == "__main__":
    tester = VolatilityBacktester(stop_loss=0.03)
    result = tester.run_backtest('005930')
    if result:
        print(result.stats())