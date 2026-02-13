from .strategy import Strategy
from Indicators.factory import IndicatorFactory

class VolatilityBreakout(Strategy):
    def __init__(self, k=0.5, stop_loss_rate=0.02):
        # 결과 폴더명 설정
        self.name = "VolatilityBreakout" 
        self.k = k
        # [추가] 손절 비율 설정 (기본값 2%)
        self.stop_loss_rate = stop_loss_rate

    def apply_strategy(self, df):
        """
        백테스트 시 전체 데이터에 대해 아래를 일괄 계산:
        1. 변동성 돌파 목표가 (전일 데이터 기반)
        2. 손절가 계산 (목표가 기준)
        3. 분석에 필요한 모든 분봉 기술적 지표
        """
        # [cite_start]1. 목표가 계산 (전일 레인지 사용) [cite: 5, 7]
        df['range'] = df['prev_high'] - df['prev_low']
        df['target_price'] = df['open'] + df['range'] * self.k

        # [추가] 2. 손절가 계산: 목표가에서 일정 비율 하락한 가격
        # 매수 진입 즉시 이 가격을 터치하면 매도하기 위함
        df['stop_price'] = df['target_price'] * (1 - self.stop_loss_rate)

        # 3. 분봉 데이터에 기술적 지표 추가
        df = IndicatorFactory.add_all_indicators(df)
        return df

    def get_signal(self, row):
        """
        분석 데이터를 기반으로 필터 조건이 추가된 시그널 판단
        """
        # 기본 조건: 현재가가 목표가 돌파
        is_breakout = row['close'] >= row['target_price']
        
        # 필터 1: 강한 상승 가속도 (d_macd_hist_slope)
        # 수익 거래 평균(99.13)이 손실 거래(25.93)보다 압도적으로 높음
        momentum_filter = row['d_macd_hist_slope'] > 30 
        
        # 필터 2: 캔들 몸통 비율 (d_body_ratio)
        # 수익 거래(0.70)가 손실 거래(0.34)보다 2배 이상 높음
        shape_filter = row['d_body_ratio'] > 0.5
        
        # 필터 3: 추세 확인 (d_band_p)
        # 수익 거래는 볼린저 밴드 상단(0.68)에 위치하는 경향이 있음
        trend_filter = row['d_band_p'] > 0.6
        
        # 필터 4: 과도한 갭 상승 제외 (d_gap_ratio)
        # 수익 거래의 갭은 마이너스(-0.0006)인 반면 손실은 플러스(0.0002)
        gap_filter = row['d_gap_ratio'] < 0.02

        # 모든 조건 충족 시 매수 시그널 발생
        if is_breakout and momentum_filter and shape_filter and trend_filter and gap_filter:
            return True
            
        return False