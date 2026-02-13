import pandas as pd
import numpy as np
import os
import sys
import matplotlib.pyplot as plt
import seaborn as sns

# 1. 프로젝트 루트 경로 등록
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.dirname(CURRENT_DIR)
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

class PerformanceStatsAnalyzer:
    def __init__(self, strategy_name="volatilitybreakout"):
        self.strategy_name = strategy_name
        self.result_dir = os.path.join(BASE_DIR, "data", "backtest", "result", self.strategy_name)
        self.summary_path = os.path.join(self.result_dir, "total_performance_summary.parquet")
        
        # 차트 한글 설정
        plt.rcParams['font.family'] = 'Malgun Gothic'
        plt.rcParams['axes.unicode_minus'] = False

    def load_summary(self):
        """통합 성과 파일을 로드합니다."""
        if not os.path.exists(self.summary_path):
            print(f"[!] 파일을 찾을 수 없습니다: {self.summary_path}")
            return None
        return pd.read_parquet(self.summary_path)

    def analyze(self, df):
        """전체 데이터를 분석하고 요약 리포트를 생성합니다."""
        print("\n" + "="*60)
        print(f" [ {self.strategy_name.upper()} 전략 통합 통계 리포트 ] ")
        print("="*60)

        # 1. 기초 통계
        report = {
            "총 분석 종목 수": len(df),
            "수익 종목 수": len(df[df['Total Return (%)'] > 0]),
            "손실 종목 수": len(df[df['Total Return (%)'] <= 0]),
            "평균 수익률 (%)": df['Total Return (%)'].mean(),
            "평균 MDD (%)": df['MDD (%)'].mean(),
            "평균 승률 (%)": df['Win Rate (%)'].mean() if 'Win Rate (%)' in df.columns else 0,
            "평균 Profit Factor": df['Profit Factor'].mean() if 'Profit Factor' in df.columns else 0
        }

        for k, v in report.items():
            print(f"{k:<20}: {v:.2f}")

        # 2. 우량 종목 필터링 (수익률 상위 + MDD 안정성)
        # 필터 기준: PF 1.2 이상, MDD -20% 이내, 거래 15회 이상
        top_tier = df[
            (df['Profit Factor'] >= 1.2) & 
            (df['MDD (%)'] >= -20.0) & 
            (df['Total Trades'] >= 15)
        ].sort_values(by='Total Return (%)', ascending=False)

        print("-" * 60)
        print(f" [*] 우량 후보 종목 선별 결과 (총 {len(top_tier)}개) ")
        print(top_tier[['Ticker', 'Total Return (%)', 'MDD (%)', 'Profit Factor', 'Avg Monthly (%)']].head(10))
        print("-" * 60)

        return top_tier

    def plot_distributions(self, df):
        """수익률 및 MDD 분포 시각화"""
        fig, axes = plt.subplots(1, 2, figsize=(15, 6))

        # 수익률 분포
        sns.histplot(df['Total Return (%)'], kde=True, ax=axes[0], color='royalblue')
        axes[0].set_title("종목별 수익률 분포 (%)")
        axes[0].axvline(0, color='red', linestyle='--')

        # 수익률 vs MDD 상관관계
        sns.scatterplot(data=df, x='MDD (%)', y='Total Return (%)', hue='Profit Factor', palette='viridis', ax=axes[1])
        axes[1].set_title("수익률 vs MDD 상관관계 (색상: PF)")
        axes[1].axhline(0, color='black', lw=1)

        plt.tight_layout()
        save_path = os.path.join(self.result_dir, "analysis_distribution.png")
        plt.savefig(save_path)
        print(f"[*] 분석 차트 저장 완료: {save_path}")
        plt.show()

# --- [ 하단 실행 코드 ] ---
if __name__ == "__main__":
    # 1. 분석기 초기화 (전략 이름에 맞춰 설정)
    analyzer = PerformanceStatsAnalyzer(strategy_name="volatilitybreakout")
    
    # 2. 데이터 로드
    summary_df = analyzer.load_summary()
    
    if summary_df is not None:
        # 3. 통계 분석 및 우량 종목 선별
        top_performers = analyzer.analyze(summary_df)
        
        # 4. 시각화 리포트 생성
        analyzer.plot_distributions(summary_df)
        
        # 5. 선별된 우량 종목 따로 저장 (CSV)
        if not top_performers.empty:
            save_name = os.path.join(analyzer.result_dir, "top_tier_candidates.csv")
            top_performers.to_csv(save_name, index=False)
            print(f"[*] 우량 종목 후보 리스트 저장 완료: {save_name}")
    else:
        print("[!] 분석할 데이터가 없습니다. batch_run.py를 먼저 실행하세요.")