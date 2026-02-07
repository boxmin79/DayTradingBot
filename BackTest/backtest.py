import pandas as pd
import numpy as np
from abc import ABC, abstractmethod
import os

class Backtest(ABC):
    """
    백테스트 수행을 위한 최상위 부모 클래스
    """
    def __init__(self, strategy):
        """
        Args:
            strategy: Strategy 클래스를 상속받은 전략 객체
        """
        self.strategy = strategy
        self.results = None

    @abstractmethod
    def run(self, df: pd.DataFrame):
        """
        데이터프레임을 입력받아 백테스트를 수행하는 추상 메서드
        """
        pass

    def calculate_metrics(self, df: pd.DataFrame):
        """
        수익률, MDD 등 주요 성과 지표를 계산하는 공통 메서드
        """
        if 'return' not in df.columns:
            return None

        # 누적 수익률 계산
        df['cum_return'] = (1 + df['return']).cumprod()
        
        # MDD(Maximum Drawdown) 계산
        df['max_cum_return'] = df['cum_return'].cummax()
        df['drawdown'] = (df['cum_return'] / df['max_cum_return']) - 1
        mdd = df['drawdown'].min()
        
        total_return = df['cum_return'].iloc[-1] - 1
        
        metrics = {
            "strategy_name": self.strategy.name,
            "total_return": f"{total_return:.2%}",
            "mdd": f"{mdd:.2%}",
            "win_rate": f"{(df['return'] > 0).mean():.2%}" if 'return' in df.columns else "N/A"
        }
        return metrics

    def save_to_csv(self, df: pd.DataFrame, ticker: str, output_dir: str = "data/backtest"):
        """
        백테스트 결과물을 CSV 파일로 저장하는 공통 메서드
        """
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
            
        file_path = os.path.join(output_dir, f"backtest_{ticker}_{self.strategy.name}.csv")
        df.to_csv(file_path)
        print(f"Result saved to: {file_path}")