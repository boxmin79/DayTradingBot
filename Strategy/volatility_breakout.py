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
        # [안전장치] 인덱스가 DatetimeIndex가 아니면 변환 시도
        if not isinstance(df.index, pd.DatetimeIndex):
            try:
                if 'date' in df.columns and 'time' in df.columns:
                    dt_str = df['date'].astype(str) + df['time'].astype(str).str.zfill(4)
                    df.index = pd.to_datetime(dt_str, format='%Y%m%d%H%M')
            except:
                pass

        # 1. 날짜 컬럼 생성
        df['date_only'] = df.index.date
        
        # 2. 일봉 데이터 생성 (전일 변동폭 계산용)
        daily = df.groupby('date_only').agg({
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last'
        })
        
        # 3. 변동폭(Range) 계산 및 전일 데이터를 오늘로 이동
        daily['range'] = daily['high'] - daily['low']
        daily['target_range'] = daily['range'].shift(1) * self.k
        
        # 4. 원본 데이터에 목표 변동폭 병합
        df = df.join(daily[['target_range']], on='date_only')
        
        # 5. 당일 시가 및 최종 목표가 계산
        df['day_open'] = df.groupby('date_only')['open'].transform('first')
        df['target_price'] = df['day_open'] + df['target_range']
        
        # 6. 매수 신호(Signal) 생성
        df['signal'] = (df['high'] >= df['target_price']).astype(int)
        
        return df

    def get_signal(self, df: pd.DataFrame):
        """
        실시간용: 가장 최신 데이터를 기준으로 돌파 여부를 반환합니다.
        """
        processed_df = self.apply_strategy(df.tail(1000)) # 최근 데이터만 처리하여 속도 향상
        if processed_df.empty:
            return False
            
        last_row = processed_df.iloc[-1]
        return last_row['high'] >= last_row['target_price']