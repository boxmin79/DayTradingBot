import pandas as pd
import numpy as np
import os
import sys
import matplotlib.pyplot as plt

# 1. 프로젝트 루트 경로 자동 등록
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.dirname(CURRENT_DIR)
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

class BacktestAnalyzer:
    def __init__(self, strategy_name):
        self.strategy_name = strategy_name.lower()
        self.result_dir = os.path.join(BASE_DIR, "data", "backtest", "result", self.strategy_name)
        plt.rcParams['font.family'] = 'Malgun Gothic'
        plt.rcParams['axes.unicode_minus'] = False

    def load_trade_details(self, ticker):
        file_path = os.path.join(self.result_dir, f"{ticker}.parquet")
        if not os.path.exists(file_path): return None
        return pd.read_parquet(file_path)

    def _prepare_dataframe(self, df):
        """날짜 변환 및 정렬 공통 로직"""
        if df is None or df.empty: return None
        
        # 복사본 생성하여 원본 보호 및 SettingWithCopyWarning 방지
        df = df.copy()
        
        # 날짜 형식 변환 (숫자형 20240101 등 대응)
        if df['date'].dtype == 'int64' or df['date'].dtype == 'float64':
            df['date'] = pd.to_datetime(df['date'].astype(str), format='%Y%m%d', errors='coerce')
        else:
            df['date'] = pd.to_datetime(df['date'], errors='coerce')
            
        return df.sort_values('date').dropna(subset=['date'])

    def calculate_statistics(self, df):
        """기존 모든 정밀 지표(PF, MDD, 기대값 등)와 월별 내역을 하나도 빠짐없이 반환"""
        df = self._prepare_dataframe(df)
        if df is None: return None, None

        # 1. 자산 곡선 및 MDD 계산
        df['equity_curve'] = (1 + df['profit']).cumprod()
        df['cumulative_max'] = df['equity_curve'].cummax()
        df['drawdown'] = (df['equity_curve'] / df['cumulative_max']) - 1
        
        profits = df['profit']
        win_trades = profits[profits > 0]
        loss_trades = profits[profits <= 0]
        
        # 2. 종합 지표 계산 (누락되었던 지표들 모두 복구)
        total_return = (df['equity_curve'].iloc[-1] - 1) * 100
        mdd = df['drawdown'].min() * 100
        win_rate = (len(win_trades) / len(df)) * 100 if len(df) > 0 else 0
        
        # Profit Factor & Recovery Factor
        profit_factor = win_trades.sum() / abs(loss_trades.sum()) if not loss_trades.empty and loss_trades.sum() != 0 else 0
        recovery_factor = abs(total_return / mdd) if mdd != 0 else 0
        
        # 기대값 (Expectancy) 및 평균 수익률
        avg_win = win_trades.mean() if not win_trades.empty else 0
        avg_loss = abs(loss_trades.mean()) if not loss_trades.empty else 0
        expectancy = ((win_rate / 100) * avg_win) - ((1 - win_rate / 100) * avg_loss)
        
        # 연속 손실 및 손절 횟수
        is_loss = profits <= 0
        consecutive_loss = is_loss.groupby((is_loss != is_loss.shift()).cumsum()).transform('sum').max()
        stop_loss_count = int(df['is_stop_loss'].sum()) if 'is_stop_loss' in df.columns else 0

        # 3. 월별 통계 및 상세 내역 (YYYY-MM)
        monthly_series = df.set_index('date')['profit'].resample('M').sum() * 100
        monthly_details = {f"{d.strftime('%Y-%m')} (%)": round(val, 2) for d, val in monthly_series.items() if val != 0}

        # 4. 결과 통합 (기존 지표 완벽 복구 + 신규 지표)
        result = {
            'Ticker': df['ticker'].iloc[0] if 'ticker' in df.columns else "Unknown",
            'Total Trades': len(df),
            'Total Return (%)': round(total_return, 2),
            'MDD (%)': round(mdd, 2),
            'Win Rate (%)': round(win_rate, 2),
            'Profit Factor': round(profit_factor, 2),
            'Recovery Factor': round(recovery_factor, 2),        # 복구
            'Expectancy (%)': round(expectancy * 100, 4),        # 복구
            'Avg Monthly (%)': round(monthly_series.mean(), 2) if not monthly_series.empty else 0,
            'Avg Win (%)': round(avg_win * 100, 2),             # 복구
            'Avg Loss (%)': round(avg_loss * 100, 2),           # 복구
            'Risk-Reward Ratio': round(abs(avg_win / avg_loss), 2) if avg_loss != 0 else 0, # 복구
            'Max Consecutive Loss': int(consecutive_loss) if not np.isnan(consecutive_loss) else 0,
            'Stop Loss Count': stop_loss_count,
            'Sharpe Ratio': round((profits.mean() / profits.std()) * np.sqrt(252), 2) if profits.std() != 0 else 0 # 복구
        }
        
        # 월별 상세 수익률 병합
        result.update(monthly_details)
        
        return result, df

    def plot_combined_chart(self, df, ticker):
        """자산 곡선 및 월별 수익률 통합 차트"""
        if df is None or 'equity_curve' not in df.columns: 
            print("[!] 차트를 그릴 데이터(equity_curve)가 없습니다.")
            return

        fig = plt.figure(figsize=(12, 10))
        gs = fig.add_gridspec(3, 1, height_ratios=[2, 1, 1.5])
        
        # 1. 누적 수익 곡선
        ax1 = fig.add_subplot(gs[0])
        ax1.plot(df['date'].values, df['equity_curve'].values, color='royalblue', lw=2)
        ax1.set_title(f"[{ticker}] 백테스트 성과 분석", fontsize=13)
        ax1.grid(True, alpha=0.3)

        # 2. 낙폭 (MDD)
        ax2 = fig.add_subplot(gs[1])
        ax2.fill_between(df['date'].values, df['drawdown'].values * 100, 0, color='red', alpha=0.3)
        ax2.set_ylabel("낙폭 (%)")
        ax2.grid(True, alpha=0.3)

        # 3. 월별 수익률 바 차트
        ax3 = fig.add_subplot(gs[2])
        monthly = df.set_index('date')['profit'].resample('M').sum() * 100
        colors = ['red' if x < 0 else 'royalblue' for x in monthly]
        ax3.bar([d.strftime('%y-%m') for d in monthly.index], monthly.values, color=colors)
        ax3.set_title("월별 수익률 (%)", fontsize=11)
        plt.xticks(rotation=45)
        
        plt.tight_layout()
        plt.show()

# --- [삼성전자 테스트 실행부] ---
if __name__ == "__main__":
    TARGET_STRATEGY = "volatilitybreakout"
    TARGET_TICKER = "005930"
    
    analyzer = BacktestAnalyzer(TARGET_STRATEGY)
    trade_df = analyzer.load_trade_details(TARGET_TICKER)
    
    if trade_df is not None:
        # stats와 업데이트된 df를 함께 받아옵니다.
        stats, updated_df = analyzer.calculate_statistics(trade_df)
        
        if stats:
            print("\n" + "="*60)
            print(f" [삼성전자(005930) 분석 결과 리포트] ")
            print("-"*60)
            
            # 주요 지표 출력
            main_keys = ['Ticker', 'Total Return (%)', 'MDD (%)', 'Win Rate (%)', 'Profit Factor', 'Avg Monthly (%)', 'Stop Loss Count']
            for k in main_keys:
                print(f"{k:<25}: {stats.get(k)}")
                
            print("-"*60)
            print(" [월별 상세 내역] ")
            month_keys = sorted([k for k in stats.keys() if '(%)' in k and '-' in k])
            for m in month_keys:
                print(f"{m:<25}: {stats[m]}")
            print("="*60)
            
            # 업데이트된 데이터프레임으로 차트 실행
            analyzer.plot_combined_chart(updated_df, TARGET_TICKER)
    else:
        print(f"\n[!] {TARGET_TICKER}.parquet 파일을 찾을 수 없습니다.")