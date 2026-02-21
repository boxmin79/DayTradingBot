import matplotlib.pyplot as plt
import seaborn as sns
import scipy.stats as stats
import os
import numpy as np
from statsmodels.graphics.tsaplots import plot_acf, plot_pacf

class TimeSeriesVisualizer():
    def __init__(self, ticker=None):
        self.ticker = ticker
        
    @staticmethod
    def set_style():
        sns.set_theme(style="whitegrid")

    @staticmethod
    def save_and_show(plt_obj, save_path, show):
        if save_path:
            os.makedirs(os.path.dirname(save_path), exist_ok=True)
            plt_obj.savefig(save_path, dpi=300, bbox_inches='tight')
            print(f"그래프 저장 완료: {save_path}")
        if show:
            plt_obj.show()
        else:
            plt_obj.close()

    def plot_normality(self, ticker=None, data=None, stats_res=None, show=True, save_path=None):
        if (data is not None) and (stats_res is not None):
            symbol = ticker if ticker else self.ticker
            """정규분포 곡선 비교 플롯"""
            self.set_style()
            plt.figure(figsize=(12, 7))
            sns.histplot(data, kde=True, color="royalblue", stat='density', bins=100, alpha=0.6)
            
            mu, std = data.mean(), data.std()
            x = np.linspace(data.min(), data.max(), 100)
            plt.plot(x, stats.norm.pdf(x, mu, std), 'r--', linewidth=2)
            
            plt.title(f"Normality Analysis: {symbol}\nSkew: {stats_res['skew']:.2f}, Kurt: {stats_res['kurt']:.2f}")
            self.save_and_show(plt, save_path, show)
        else:
            print("데이터가 없습니다.")

    def plot_hurst(self, ticker=None, hurst_res=None, show=True, save_path=None):
        if hurst_res is not None:
            symbol = ticker if ticker else self.ticker
            """허스트 지수 Log-Log 플롯"""
            
            # 딕셔너리로 변경된 반환값에서 각각의 키(Key)로 데이터를 꺼내옵니다.
            h_val = hurst_res['hurst']
            r2_val = hurst_res['r_squared']
            intercept = hurst_res['intercept']
            log_n = hurst_res['log_n']
            log_rs = hurst_res['log_rs']
            
            # 선택 사항: p_value도 같이 받아올 수 있습니다.
            # p_val = hurst_res['p_value']
            
            self.set_style()
            plt.figure(figsize=(10, 6))
            plt.scatter(log_n, log_rs, color='royalblue', alpha=0.7, label='Log(R/S)')
            
            # 선형 회귀선 그리기 (y = 기울기 * x + y절편)
            plt.plot(log_n, h_val * log_n + intercept, 'crimson', linestyle='--', label=f'Fit (H={h_val:.3f})')
            
            # 그래프 제목 및 라벨 설정
            plt.title(f"Hurst Analysis: {symbol} (H={h_val:.3f}, R²={r2_val:.3f})")
            plt.xlabel('Log(n)')
            plt.ylabel('Log(R/S)')
            plt.legend()
            
            self.save_and_show(plt, save_path, show)
        else:
            print(f"[{ticker if ticker else self.ticker}] 시각화할 데이터가 없습니다.")
            
    def plot_qq(self, ticker=None, data=None, show=True, save_path=None):
        if data is not None:
            symbol = ticker if ticker else self.ticker
            """Q-Q 플롯"""
            self.set_style()
            plt.figure(figsize=(8, 8))
            stats.probplot(data, dist="norm", plot=plt)
            plt.title(f"Q-Q Plot: {symbol}")
            self.save_and_show(plt, save_path, show)
        else:
            print("데이터가 없습니다.")
    
    def plot_autocorrelation(self, ticker=None, log_returns=None, lags=40, show=True, save_path=None):
        """ACF 및 PACF 그래프 시각화"""
        if log_returns is not None:
            symbol = ticker if ticker else self.ticker
            self.set_style()
            fig, ax = plt.subplots(1, 2, figsize=(15, 6))
            
            # ACF 시각화
            plot_acf(log_returns, lags=lags, ax=ax[0], color='royalblue', alpha=0.05)
            ax[0].set_title(f"ACF: {symbol}")
            
            # PACF 시각화
            plot_pacf(log_returns, lags=lags, ax=ax[1], color='crimson', alpha=0.05)
            ax[1].set_title(f"PACF: {symbol}")
            
            plt.tight_layout()
            self.save_and_show(plt, save_path, show)
        else:
            print("시각화할 수익률 데이터가 없습니다.")
            
