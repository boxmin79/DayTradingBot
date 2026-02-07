import pandas as pd
from .strategy import Strategy

class VolatilityBreakout(Strategy):
    """
    변동성 돌파 전략 (Volatility Breakout Strategy)
    로직: 매수 목표가 = 당일 시가 + (전일 고가 - 전일 저가) * k
    """
    def __init__(self, k=0.5):
        super().__init__(name="VolatilityBreakout")
        self.k = k

    def apply_strategy(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        백테스트용: 전체 분봉 데이터에 전략 지표를 추가합니다.
        """
        # 1. 날짜 컬럼 생성 (인덱스가 DatetimeIndex라고 가정)
        df['date'] = df.index.date
        
        # 2. 일봉 데이터 생성 (전일 변동폭 계산용)
        daily = df.groupby('date').agg({
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last'
        })
        
        # 3. 변동폭(Range) 및 목표가(Target Range) 계산
        daily['range'] = daily['high'] - daily['low']
        # 전일 변동폭을 오늘로 가져옴 (shift 1)
        daily['target_range'] = daily['range'].shift(1) * self.k
        
        # 4. 원본 데이터에 목표 변동폭 병합
        df = df.join(daily[['target_range']], on='date')
        
        # 5. 당일 시가 및 최종 목표가 계산
        df['day_open'] = df.groupby('date')['open'].transform('first')
        df['target_price'] = df['day_open'] + df['target_range']
        
        # 6. 매수 신호(Signal) 생성
        # 고가가 목표가를 돌파하는 순간 신호 발생 (1)
        df['signal'] = (df['high'] >= df['target_price']).astype(int)
        
        return df

    def get_signal(self, df: pd.DataFrame):
        """
        실시간용: 가장 최신 데이터를 기준으로 돌파 여부를 반환합니다.
        """
        processed_df = self.apply_strategy(df)
        if processed_df.empty:
            return False
            
        last_row = processed_df.iloc[-1]
        return last_row['high'] >= last_row['target_price']