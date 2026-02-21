import vectorbt as vbt
import pandas as pd
import os

class VolatilityBacktester:
    def __init__(self, slippage=0.001, fees=0.00015, tax=0.002, stop_loss=0.03, k=0.5):
        # 경로를 f-string이나 os.path.join으로 관리하면 더 안전합니다.
        self.daily_path = os.path.join("data","chart","daily")
        self.minute_path = os.path.join("data","chart","minute")
        print(self.daily_path)
        print(self.minute_path)
        

        # self.daily_path = "C:/Users/realb/Documents/DayTradingBot/data/chart/daily"
        # self.minute_path = "C:/Users/realb/Documents/DayTradingBot/data/chart/minute"
        self.k = k
        self.fees = fees
        self.tax = tax
        self.stop_loss = stop_loss
        self.slippage = slippage

    def _load_data(self, ticker):
        """데이터 로드 및 'date' 컬럼을 인덱스로 설정"""
        d_path = os.path.join(self.daily_path, f"{ticker}.parquet")
        m_path = os.path.join(self.minute_path, f"{ticker}.parquet")
        
        d_df = pd.read_parquet(d_path)
        m_df = pd.read_parquet(m_path)
        
        # [핵심 수정] 'date' 컬럼이 있다면 인덱스로 지정
        if 'date' in d_df.columns:
            d_df = d_df.set_index('date')
            
        if 'date' in m_df.columns:
            m_df = m_df.set_index('date')

        # 혹시 모르니 인덱스를 확실하게 datetime으로 변환 (안전장치)
        if d_df.index.dtype != 'datetime64[ns]':
            d_df.index = pd.to_datetime(d_df.index)
            
        if m_df.index.dtype != 'datetime64[ns]':
            m_df.index = pd.to_datetime(m_df.index)
        
        
        # 중복 제거 및 정렬
        d_df = d_df[~d_df.index.duplicated(keep='last')].sort_index()
        m_df = m_df[~m_df.index.duplicated(keep='first')].sort_index()

        return d_df, m_df

    def run_backtest(self, ticker):
        """특정 종목에 대해 분봉 기반 백테스트 실행 (디버깅 포함)"""
        try:
            d_df, m_df = self._load_data(ticker)
            
            # --- 1. 일봉 기준 타겟가 계산 ---
            prev_range = (d_df['high'] - d_df['low']).shift(1)
            target_price = d_df['open'] + prev_range * self.k
            
            # --- 2. 분봉 데이터에 타겟가 매핑 ---
            m_df['date'] = m_df.index.date
            
            # 매핑을 위한 시리즈 준비
            target_map = target_price.copy()
            target_map.index = target_map.index.date
            
            # 매핑 실행
            m_df['target_price'] = m_df['date'].map(target_map)
            
            # [디버깅 1] 타겟가가 제대로 들어갔는지 확인
            nan_count = m_df['target_price'].isna().sum()
            total_count = len(m_df)
            print(f"[{ticker}] 전체 데이터: {total_count}행, 타겟가 NaN 개수: {nan_count}행")
            
            if nan_count == total_count:
                print(f"🚨 오류: 일봉 데이터와 분봉 데이터의 날짜가 매칭되지 않습니다!")
                print(f"일봉 날짜 예시: {d_df.index[0]}")
                print(f"분봉 날짜 예시: {m_df.index[0]}")
                return None

            # --- 3. 신호 생성 ---
            entries = m_df['high'] >= m_df['target_price']
            exits = m_df.groupby('date').cumcount(ascending=False) == 0
            
            # [디버깅 2] 매수 신호가 한번이라도 떴는지 확인
            entry_count = entries.sum()
            print(f"[{ticker}] 생성된 매수 신호(entries) 개수: {entry_count}회")
            
            if entry_count == 0:
                print("💡 원인: 조건(고가 > 타겟가)을 만족하는 순간이 없었습니다.")
                # 확인을 위해 데이터 일부 출력
                print(m_df[['high', 'target_price']].head(10))
                return None

            # --- 4. 시뮬레이션 (수정된 인자 적용) ---
            # 실행 가격 설정 (진입 시 타겟가, 그 외 종가)
            exec_price = m_df['close'].copy()
            exec_price.loc[entries] = m_df['target_price']

            pf = vbt.Portfolio.from_signals(
                close=m_df['close'],
                entries=entries,
                exits=exits,
                price=exec_price,
                fees=self.fees,
                slippage=self.slippage,
                # init_cash=1000000, 
                freq='m' 
            )
            return pf
            
        except Exception as e:
            print(f"Error analyzing {ticker}: {e}")
            import traceback
            traceback.print_exc() # 에러 상세 출력
            return None

if __name__ == "__main__":
    tester = VolatilityBacktester(stop_loss=0.03) # 손절 3% 설정
    result = tester.run_backtest('005930')
    
    if result:
        print(result.stats())
        result.plot().show()
    