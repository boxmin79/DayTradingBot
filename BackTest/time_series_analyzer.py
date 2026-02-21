import os
import pandas as pd
import numpy as np
import scipy.stats as stats
import matplotlib.pyplot as plt
import seaborn as sns
from time_series_visualizer import TimeSeriesVisualizer

class TimeSeriesAnalyzer():
    def __init__(self, ticker=None):
        self.ticker = ticker # 객체 생성 시 저장된 기본값 (없을 수도 있음)
        self.viz = TimeSeriesVisualizer(ticker)

    def get_df(self, ticker=None, dir_path=None):
        """데이터프레임 로드 (최하단 메서드)"""
        # 호출 시 받은 ticker가 있으면 그것을 쓰고, 없으면 객체 기본값을 사용
        symbol = ticker if ticker else self.ticker
        if not symbol:
            print("알림: 분석할 티커가 지정되지 않았습니다.")
            return None # 에러 대신 None 반환
        
        
        if dir_path is None:
            dir_path = os.path.join(".", "data", "chart", "daily")
        file_path = os.path.join(dir_path, f"{symbol}.parquet")
        
        if not os.path.exists(file_path):
            print(f"파일 없음: {file_path}")
            return None

        df = pd.read_parquet(file_path, engine="pyarrow")
        df['date'] = pd.to_datetime(df['date'])
        df.set_index('date', inplace=True)
        return df 

    def get_log_rtns(self, ticker=None):
        """로그 수익률 계산 (중간 단계)"""
        # 1. 사용할 종목 확정
        symbol = ticker if ticker else self.ticker
        # 2. 확정된 symbol을 하부 메서드에 명시적으로 전달 (혼선 방지 핵심)
        df = self.get_df(ticker=symbol)
        
        if df is None: return None
        close_col = 'close' if 'close' in df.columns else 'Close'
        return np.log(df[close_col] / df[close_col].shift(1)).dropna()

    def calculate_hurst(self, ticker=None):
        """
        허스트 지수 및 시각화용 데이터 계산
        Returns:
            H, R2, intercept, log_n, log_rs
        """
        symbol = ticker if ticker else self.ticker
        df = self.get_df(ticker=symbol)
        # 반환 개수를 5개로 맞춰서 에러 방지
        if df is None: return None, None, None, None, None
        
        close_col = 'close' if 'close' in df.columns else 'Close'
        series = df[close_col].values
        
        if len(series) < 50:
            print(f"[{symbol}] 데이터 부족 (최소 50개 필요)")
            return None, None, None, None, None

        def __get_rs(n):
            num_batches = len(series) // n
            rs_list = []
            for i in range(num_batches):
                batch = series[i*n : (i+1)*n]
                mean_adj = batch - np.mean(batch)
                cum_sum = np.cumsum(mean_adj)
                r = np.max(cum_sum) - np.min(cum_sum)
                s = np.std(batch)
                if s > 0:
                    rs_list.append(r / s)
            return np.mean(rs_list) if rs_list else 0

        n_values = np.unique(np.logspace(1, np.log10(len(series)//2), 15).astype(int))
        rs_values = np.array([__get_rs(n) for n in n_values])
        
        valid = rs_values > 0
        if not any(valid): return None, None, None, None, None
        
        log_n = np.log(n_values[valid])
        log_rs = np.log(rs_values[valid])
        
        # 선형 회귀 분석
        slope, intercept, r_value, p_value, std_err = stats.linregress(log_n, log_rs)
        
        # 그래프를 그리기 위해 intercept, log_n, log_rs를 추가로 반환합니다.
        return slope, r_value**2, intercept, log_n, log_rs

    def normal_test(self, ticker=None):
        symbol = ticker if ticker else self.ticker
        log_rtns = self.get_log_rtns(ticker=symbol)
        if log_rtns is None: return None
        
        # 1. 기본 통계량 (왜도, 첨도)
        skew = log_rtns.skew()
        kurt = log_rtns.kurt() 
        
        # 2. 3종 정규성 검정
        shapiro_stat, shapiro_p = stats.shapiro(log_rtns)
        jb_stat, jb_p = stats.jarque_bera(log_rtns)
        
        # KS Test (표준화 후 진행)
        data_std = (log_rtns - log_rtns.mean()) / log_rtns.std()
        ks_stat, ks_p = stats.kstest(data_std, 'norm')
        
        return {
            "skew": skew, "kurt": kurt, 
            "shapiro_stat": shapiro_stat, "shapiro_p": shapiro_p,
            "jb_stat": jb_stat, "jb_p": jb_p, 
            "ks_stat": ks_stat, "ks_p": ks_p
        }
            
    
            
if __name__ == "__main__":
    # 시나리오 A: 범용 도구 (ticker 없이 생성)
    tool = TimeSeriesAnalyzer("005930")
    h_res = tool.calculate_hurst()
    log_rtns = tool.get_log_rtns()
    normal_res = tool.normal_test()
    
    
    tool.viz.plot_hurst(hurst_res=h_res)
    tool.viz.plot_normality(data=log_rtns, stats_res=normal_res)
    tool.viz.plot_qq(data=log_rtns)
    
