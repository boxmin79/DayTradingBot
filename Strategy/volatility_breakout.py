from .strategy import Strategy

class VolatilityBreakout(Strategy):
    def __init__(self, k=0.5):
        # 결과 폴더명 설정
        self.name = "VolatilityBreakout" 
        self.k = k

    def apply_strategy(self, df):
        """백테스트 시 전체 데이터에 대해 목표가(target_price)를 일괄 계산"""
        df['range'] = df['high'].shift(1) - df['low'].shift(1)
        df['target_price'] = df['open'] + df['range'] * self.k
        return df

    def get_signal(self, row):
        """
        [추가된 부분] 실시간 매매 시 시그널을 판단하기 위한 추상 메서드 구현.
        현재가가 목표가보다 높으면 True(매수)를 반환합니다.
        """
        if row['close'] >= row['target_price']:
            return True
        return False