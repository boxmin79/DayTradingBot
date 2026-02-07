import pandas as pd
from abc import ABC, abstractmethod

class Strategy(ABC):
    """
    주가 데이터를 기반으로 전략을 수행하는 최상위 부모 클래스
    """
    def __init__(self, name: str):
        self.name = name

    @abstractmethod
    def apply_strategy(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        주가 데이터프레임을 입력받아 전략 로직(지표 계산, 신호 생성 등)을 적용합니다.
        
        Args:
            df (pd.DataFrame): 'open', 'high', 'low', 'close', 'volume' 등을 포함한 주가 데이터
            
        Returns:
            pd.DataFrame: 전략 로직이 반영된 데이터프레임
        """
        pass

    @abstractmethod
    def get_signal(self, df: pd.DataFrame):
        """
        데이터프레임의 가장 최근 행(현재 시점)을 기준으로 매수/매도 신호를 판단합니다.
        """
        pass