import pandas as pd
import numpy as np
from abc import ABC, abstractmethod
import os

class Backtest(ABC):
    def __init__(self, strategy):
        self.strategy = strategy
        self.results = None

    @abstractmethod
    def run(self, df: pd.DataFrame):
        pass

    def calculate_metrics(self, df: pd.DataFrame):
        """
        심화 분석 지표(수익성, 안정성, 일관성, 분포) 통합 계산
        """
        if df.empty or 'return' not in df.columns or (df['return'] == 0).all():
            return None

        # 1. 수익성 및 리스크
        df['cum_return'] = (1 + df['return']).cumprod()
        total_return = df['cum_return'].iloc[-1] - 1
        mdd = ((df['cum_return'] / df['cum_return'].cummax()) - 1).min()
        
        # 2. 매매 기록
        trades = df[df['return'] != 0].copy()
        num_trades = len(trades)
        wins = trades[trades['return'] > 0]['return']
        losses = trades[trades['return'] < 0]['return']
        
        win_rate = len(wins) / num_trades
        avg_profit = wins.mean() if not wins.empty else 0
        avg_loss = losses.mean() if not losses.empty else 0
        
        # 3. 일관성 및 분포 분석
        # [수익 일관성] 상위 5% 제외 수익률
        if num_trades > 10:
            threshold = trades['return'].quantile(0.95)
            adj_return = (1 + trades[trades['return'] < threshold]['return']).prod() - 1
        else:
            adj_return = total_return

        # [매매 분포] 월별 매매 횟수 변동계수
        trades['month'] = pd.to_datetime(trades.index).to_period('M')
        monthly_counts = trades.groupby('month').size()
        consistency_score = monthly_counts.std() / monthly_counts.mean() if not monthly_counts.empty else 9.9
        
        # 4. 통계적 우위
        expectancy = (win_rate * avg_profit) + ((1 - win_rate) * avg_loss)
        std = trades['return'].std()
        sqn = (trades['return'].mean() / std) * np.sqrt(num_trades) if std != 0 else 0

        metrics = {
            "총수익률": f"{total_return:.2%}",
            "조정수익률": f"{adj_return:.2%}",
            "MDD": f"{mdd:.2%}",
            "기대값": f"{expectancy:.4f}",
            "SQN": f"{sqn:.2f}",
            "매매분포지수": f"{consistency_score:.2f}",
            "승률": f"{win_rate:.2%}",
            "매매횟수": num_trades
        }
        return metrics

    def save_to_csv(self, df: pd.DataFrame, ticker: str, metrics: dict = None):
        output_dir = "data/backtest"
        if not os.path.exists(output_dir): os.makedirs(output_dir)
        file_path = os.path.join(output_dir, f"backtest_{ticker}_{self.strategy.name}.csv")
        df.to_csv(file_path, index=True, encoding='utf-8-sig')
        if metrics:
            with open(file_path, 'a', encoding='utf-8-sig') as f:
                f.write("\n" + "="*50 + "\n   [통합 성과 보고서]\n" + "="*50 + "\n")
                for k, v in metrics.items(): f.write(f"{k}: {v}\n")