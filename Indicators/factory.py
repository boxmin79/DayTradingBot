import pandas as pd
import numpy as np

class IndicatorFactory:
    """
    기술적 지표 생성기: 
    사용자가 정의한 4가지 분석 관점(추세, 모멘텀, 변동성, 거래량)을 기반으로 지표를 생성합니다.
    """

    @staticmethod
    def add_all_indicators(df: pd.DataFrame) -> pd.DataFrame:
        """모든 주요 기술적 지표를 데이터프레임에 추가합니다."""
        df = IndicatorFactory.add_trend(df)
        df = IndicatorFactory.add_momentum(df)
        df = IndicatorFactory.add_volatility(df)
        df = IndicatorFactory.add_volume(df)
        df = IndicatorFactory.add_advanced(df)
        return df

    @staticmethod
    def add_trend(df: pd.DataFrame) -> pd.DataFrame:
        """1. 추세 강도 및 이동평균선 분석"""
        # 이동평균선 (정배열/역배열 확인용)
        df['ma5'] = df['close'].rolling(window=5).mean()
        df['ma20'] = df['close'].rolling(window=20).mean()
        df['ma60'] = df['close'].rolling(window=60).mean()
        df['ma120'] = df['close'].rolling(window=120).mean()

        # 이격도 (현재가가 이평선 대비 얼마나 떨어져 있는가)
        df['disparity20'] = (df['close'] / df['ma20']) * 100
        
        # 추세 강도 (ADX 간이 계산 - 단순 방향성 지표)
        # 실제 ADX는 복잡하므로 여기서는 전일 대비 상승/하락 연속성을 활용한 강도 지표 예시
        df['price_change'] = df['close'].diff()
        df['trend_strength'] = df['price_change'].rolling(window=14).mean() / df['price_change'].rolling(window=14).std()
        
        return df

    @staticmethod
    def add_momentum(df: pd.DataFrame) -> pd.DataFrame:
        """2. 과매수/과매도 및 모멘텀 (RSI, MACD)"""
        # RSI (14일 기준)
        delta = df['close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
        rs = gain / loss
        df['rsi'] = 100 - (100 / (1 + rs))

        # MACD (12, 26, 9)
        exp12 = df['close'].ewm(span=12, adjust=False).mean()
        exp26 = df['close'].ewm(span=26, adjust=False).mean()
        df['macd'] = exp12 - exp26
        df['macd_signal'] = df['macd'].ewm(span=9, adjust=False).mean()
        df['macd_hist'] = df['macd'] - df['macd_signal']
        
        # MACD 히스토그램 기울기 (추세 약화 감지용)
        df['macd_hist_slope'] = df['macd_hist'].diff()
        
        return df

    @staticmethod
    def add_volatility(df: pd.DataFrame) -> pd.DataFrame:
        """3. 변동성 및 가격 위치 (ATR, 볼린저 밴드)"""
        # ATR (Average True Range) - 14일 기준
        high_low = df['high'] - df['low']
        high_close = np.abs(df['high'] - df['close'].shift())
        low_close = np.abs(df['low'] - df['close'].shift())
        ranges = pd.concat([high_low, high_close, low_close], axis=1)
        true_range = ranges.max(axis=1)
        df['atr'] = true_range.rolling(window=14).mean()

        # 볼린저 밴드 (20일, 2표준편차)
        df['stddev'] = df['close'].rolling(window=20).std()
        df['bb_upper'] = df['ma20'] + (df['stddev'] * 2)
        df['lower_band'] = df['ma20'] - (df['stddev'] * 2)
        
        # 밴드 내 위치 (%B)
        df['band_p'] = (df['close'] - df['lower_band']) / (df['bb_upper'] - df['lower_band'])
        
        return df

    @staticmethod
    def add_volume(df: pd.DataFrame) -> pd.DataFrame:
        """4. 거래량 유효성 분석"""
        # 최근 20일 평균 거래량 대비 비율
        df['volume_ma20'] = df['volume'].rolling(window=20).mean()
        df['volume_ratio'] = (df['volume'] / df['volume_ma20']) * 100

        # OBV (On-Balance Volume)
        df['obv'] = (np.sign(df['close'].diff()) * df['volume']).fillna(0).cumsum()
        
        return df
    
    @staticmethod
    def add_advanced(df: pd.DataFrame) -> pd.DataFrame:
        """에너지 및 가격 위치 심화 분석"""
        # 1. MFI (Money Flow Index) - 거래량 포함 RSI
        typical_price = (df['high'] + df['low'] + df['close']) / 3
        money_flow = typical_price * df['volume']
        
        # Positive/Negative Money Flow
        delta = typical_price.diff()
        pos_flow = pd.Series(0.0, index=df.index)
        neg_flow = pd.Series(0.0, index=df.index)
        
        pos_flow[delta > 0] = money_flow[delta > 0]
        neg_flow[delta < 0] = money_flow[delta < 0]
        
        pos_mf = pos_flow.rolling(window=14).sum()
        neg_mf = neg_flow.rolling(window=14).sum()
        
        mfi_ratio = pos_mf / neg_mf
        df['mfi'] = 100 - (100 / (1 + mfi_ratio))

        # 2. 캔들 몸통 비율 (Body Ratio)
        df['body_ratio'] = (df['close'] - df['open']).abs() / (df['high'] - df['low'])
        
        # 3. 전일 종가 대비 시가 갭 (Gap)
        df['gap_ratio'] = (df['open'] - df['close'].shift(1)) / df['close'].shift(1)
        
        return df
    
    @staticmethod
    def add_custom_indicators(df: pd.DataFrame) -> pd.DataFrame:
        """사용자 요청 지표: 밴드폭, 전일 돌파, VWAP, 거래대금 추가"""
        
        # 1. 볼린저 밴드 폭 (Bandwidth)
        # 밴드가 수축(Squeeze)하는지 발산하는지 측정
        df['bb_width'] = (df['bb_upper'] - df['lower_band']) / df['ma20']
        
        # 2. 전일 고가/저가 돌파 여부
        # 고가 돌파: 1, 저가 이탈: -1, 유지: 0
        df['prev_high'] = df['high'].shift(1)
        df['prev_low'] = df['low'].shift(1)
        df['break_high'] = (df['close'] > df['prev_high']).astype(int)
        df['break_low'] = (df['close'] < df['prev_low']).astype(int) * -1
        
        # 3. VWAP (Volume Weighted Average Price)
        # 일봉 기준으로는 20일 누적 거래대금을 누적 거래량으로 나누어 계산
        # (분봉일 경우 당일 누적으로직으로 변경 필요)
        v = df['volume']
        tp = (df['high'] + df['low'] + df['close']) / 3  # Typical Price
        
        if 'time' in df.columns and 'date' in df.columns:
            # 분봉 데이터(date, time 컬럼 존재)인 경우: 당일 누적 VWAP 계산
            tp_v = tp * v
            cum_tp_v = tp_v.groupby(df['date']).cumsum()
            cum_v = v.groupby(df['date']).cumsum()
            df['vwap'] = cum_tp_v / cum_v
        else:
            # 일봉 데이터 혹은 date 컬럼이 없는 경우: 20일 이동 VWAP
            df['vwap'] = (tp * v).rolling(window=20).sum() / v.rolling(window=20).sum()
        
        # 4. 거래대금 (Trading Value)
        # 단위가 너무 커질 수 있어 보통 10억(1e9)이나 1백만(1e6)으로 나눕니다.
        df['trading_value'] = df['close'] * df['volume']
        
        return df