import pandas as pd
from abc import ABC, abstractmethod

class Strategy(ABC):
    """
    주가 데이터를 기반으로 전략을 수행하는 최상위 부모 클래스
    """
    def __init__(self, name: str, params: dict = None):
        self.name = name
        self.params = params if params else {}
        # 전략 구동에 필요한 최소 데이터 행 수 (예: 20일 이평선은 최소 20행)
        self.min_data_required = self.params.get('min_data', 1)

    @abstractmethod
    def apply_strategy(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        [백테스트용] 전체 데이터프레임에 전략 지표 및 신호를 생성합니다.
        """
        pass

    @abstractmethod
    def get_signal(self, df: pd.DataFrame):
        """
        [실시간용] 현재 시점의 데이터를 바탕으로 매수(True)/매도(False) 신호를 반환합니다.
        """
        # 데이터 부족 시 처리 로직을 부모에서 공통으로 가질 수 있음
        if len(df) < self.min_data_required:
            return False
        pass

    def calculate_common_indicators(self, df: pd.DataFrame):
        """
        [공통 기능] 이동평균선, RSI 등 여러 전략에서 자주 쓰는 지표를 계산합니다.
        필요 시 자식 클래스에서 super().calculate_common_indicators(df)로 호출
        """
        # 예시: 종가 기준 5일, 20일 이동평균선
        df['ma5'] = df['close'].rolling(window=5).mean()
        df['ma20'] = df['close'].rolling(window=20).mean()
        return df