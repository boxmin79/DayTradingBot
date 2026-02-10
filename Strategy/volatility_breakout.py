from .strategy import Strategy
from Indicators.factory import IndicatorFactory

class VolatilityBreakout(Strategy):
    def __init__(self, k=0.5):
        # 결과 폴더명 설정
        self.name = "VolatilityBreakout" 
        self.k = k

    def apply_strategy(self, df):
        """
        백테스트 시 전체 데이터에 대해 아래를 일괄 계산:
        1. 변동성 돌파 목표가 (전일 데이터 기반)
        2. 분석에 필요한 모든 분봉 기술적 지표
        """
        # 1. 목표가 계산 (전일 레인지 사용)
        df['range'] = df['prev_high'] - df['prev_low']
        df['target_price'] = df['open'] + df['range'] * self.k

        # 2. 분봉 데이터에 기술적 지표 추가
        df = IndicatorFactory.add_all_indicators(df)
        return df

    def get_signal(self, row):
        """
        [추가된 부분] 실시간 매매 시 시그널을 판단하기 위한 추상 메서드 구현.
        현재가가 목표가보다 높으면 True(매수)를 반환합니다.
        """
        if row['close'] >= row['target_price']:
            return True
        return False