import pandas as pd
import numpy as np
import os
from abc import ABC, abstractmethod

class Backtest(ABC):
    def __init__(self, strategy):
        self.strategy = strategy
        self.base_path = os.path.join("data", "backtest", self.strategy.name)
        self.summary_path = os.path.join(self.base_path, "summary")
        os.makedirs(self.summary_path, exist_ok=True)
        
        # [추가] 분석된 종목들의 요약을 담을 리스트
        self.total_summary_list = []
        print(f"    [엔진] 보고서 저장 경로 준비: {self.base_path}")

    @abstractmethod
    def run(self, df: pd.DataFrame, ticker=None):
        pass

    def add_summary(self, metrics):
        """분석된 종목의 지표를 엔진 내부 리스트에 추가"""
        if metrics:
            self.total_summary_list.append(metrics)

    def save_total_report(self):
        """엔진에 쌓인 모든 결과를 합쳐서 수익률 순으로 CSV 저장"""
        if not self.total_summary_list:
            return
            
        report_path = os.path.join(self.base_path, "total_investment_report.csv")
        df = pd.DataFrame(self.total_summary_list)
        
        # 수익률 기준 내림차순 정렬 (문자열 % 제거 후 계산)
        if '총수익률' in df.columns:
            df['sort_val'] = df['총수익률'].str.replace('%','').astype(float)
            df = df.sort_values(by='sort_val', ascending=False).drop(columns=['sort_val'])
        
        df.to_csv(report_path, index=False, encoding='utf-8-sig')
        print(f"\n    [엔진] 통합 리포트 업데이트 완료: {len(df)} 종목 기록됨")

    def calculate_metrics(self, df: pd.DataFrame):
        """성과 지표 계산 로직"""
        if df.empty or 'return' not in df.columns: return None
        df['cum_return'] = (1 + df['return']).cumprod()
        total_return = df['cum_return'].iloc[-1] - 1
        mdd = ((df['cum_return'] / df['cum_return'].cummax()) - 1).min()
        trades = df[df['return'] != 0]
        if len(trades) == 0: return None
        win_rate = len(trades[trades['return'] > 0]) / len(trades)
        
        return {
            "총수익률": f"{total_return:.2%}",
            "MDD": f"{mdd:.2%}",
            "승률": f"{win_rate:.2%}",
            "매매횟수": len(trades),
            "SQN": f"{(trades['return'].mean() / trades['return'].std() * np.sqrt(len(trades))):.2f}" if len(trades) > 1 else "0.00",
            "기대값": f"{trades['return'].mean():.4f}"
        }

    def save_results(self, result_df, metrics, ticker):
        """개별 종목 파일 저장"""
        csv_path = os.path.join(self.base_path, f"backtest_{ticker}.csv")
        result_df.to_csv(csv_path, encoding='utf-8-sig')
        
        txt_path = os.path.join(self.summary_path, f"summary_{ticker}.txt")
        with open(txt_path, 'w', encoding='utf-8-sig') as f:
            f.write(f"--- [ {ticker} / {self.strategy.name} 분석 ] ---\n")
            for k, v in metrics.items(): f.write(f"{k}: {v}\n")