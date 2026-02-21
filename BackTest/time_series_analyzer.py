import os
import pandas as pd
import numpy as np
import scipy.stats as stats
import matplotlib.pyplot as plt
from statsmodels.tsa.stattools import adfuller
from statsmodels.graphics.tsaplots import plot_acf, plot_pacf
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
        
        # 1. 만들어둔 메서드를 호출하여 로그 수익률 데이터 가져오기
        log_returns = self.get_log_rtns(ticker=symbol)
        
        # 2. 데이터가 없거나 정상적으로 불러오지 못한 경우 에러 방지
        if log_returns is None or log_returns.empty: 
            return None, None, None, None, None
        
        # 3. 연산을 위해 순수 numpy 배열(1차원)로 변환
        series = log_returns.values
        
        # 4. 최소 데이터 개수 확인
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
        
        # 결과를 딕셔너리 형태로 묶어서 반환
        return {
            'hurst': slope,           # 허스트 지수 (H)
            'r_squared': r_value**2,  # R^2 (설명력)
            'intercept': intercept,   # Y절편 (그래프용)
            'log_n': log_n,           # X축 데이터 (그래프용)
            'log_rs': log_rs,         # Y축 데이터 (그래프용)
            'p_value': p_value,       # 유의 확률 (신뢰도 검증용)
            'std_err': std_err        # 표준 오차 (신뢰도 검증용)
        }

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
            
    def adf_test(self, ticker=None):
        """
        로그 수익률을 사용하여 ADF(Augmented Dickey-Fuller) 검정을 수행합니다.
        가상환경에 statsmodels 패키지가 설치되어 있어야 합니다.
        """
        # 1. 대상 종목 확정 및 로그 수익률 가져오기
        symbol = ticker if ticker else self.ticker
        # 기존에 구현된 get_log_rtns 메서드를 호출하여 일관성 유지
        log_rtns = self.get_log_rtns(ticker=symbol)
        
        if log_rtns is None or len(log_rtns) < 20:
            print(f"[{symbol}] 데이터가 부족하여 ADF 검정을 수행할 수 없습니다.")
            return None

        # 2. ADF 검정 실행 (statsmodels 라이브러리 활용)
        result = adfuller(log_rtns)
        
        # 3. 결과 정리 및 반환
        return {
            'statistics': result[0],       # 검정 통계량
            'p-value': result[1],          # p-value (0.05 미만일 경우 정상성 확보)
            'used_lag': result[2],         # 사용된 시차(lag) 수
            'n_obs': result[3],            # 관측치 개수
            'critical_values': result[4],  # 임계값 (1%, 5%, 10%)
            'is_stationary': result[1] < 0.05  # 정상성 여부 (True/False)
        }
        
            
if __name__ == "__main__":
    # 분석할 종목 티커 설정 (예: 삼성전자 "005930")
    ticker = "005930"
    tool = TimeSeriesAnalyzer(ticker)
    
    print(f"========== [{ticker}] 시계열 데이터 분석 결과 ==========")

    # 1. 허스트 지수 (Hurst Exponent) 결과 출력
    hurst_res = tool.calculate_hurst()
    if hurst_res:
        print("\n[1] 허스트 지수 (Hurst Exponent) 분석")
        print(f"  - Hurst 지수 (H): {hurst_res['hurst']:.4f}")
        print(f"  - 설명력 (R²): {hurst_res['r_squared']:.4f}")
        print(f"  - 유의 확률 (p-value): {hurst_res['p_value']:.4e}")
        print(f"  - 표준 오차 (std_err): {hurst_res['std_err']:.4e}")
        
        # 허스트 지수 해석
        h = hurst_res['hurst']
        if h > 0.55:
            print("  ▶ 해석: 추세성(Persistent)이 존재합니다. (모멘텀 전략 유리)")
        elif h < 0.45:
            print("  ▶ 해석: 평균 회귀성(Anti-persistent)이 존재합니다. (역추세/박스권 전략 유리)")
        else:
            print("  ▶ 해석: 랜덤 워크(Random Walk)에 가깝습니다. (과거 가격으로 미래 예측 어려움)")
    else:
        print("\n[1] 허스트 지수 분석 실패 (데이터 부족 등)")

    # 2. 정규성 검정 (Normality Test) 결과 출력
    norm_res = tool.normal_test()
    if norm_res:
        print("\n[2] 수익률 분포 정규성 검정")
        print(f"  - 왜도 (Skewness): {norm_res['skew']:.4f} (0에 가까울수록 정규분포)")
        print(f"  - 첨도 (Kurtosis): {norm_res['kurt']:.4f} (3에 가까울수록 정규분포)")
        print(f"  - Shapiro-Wilk p-value: {norm_res['shapiro_p']:.4e}")
        print(f"  - Jarque-Bera p-value: {norm_res['jb_p']:.4e}")
        print(f"  - K-S Test p-value: {norm_res['ks_p']:.4e}")
        
        # p-value가 0.05보다 크면 정규분포를 따른다고 해석 (보통 금융데이터는 안 따름)
        if norm_res['jb_p'] < 0.05:
            print("  ▶ 해석: 정규분포를 따르지 않습니다. (Fat-tail 등 금융 데이터의 전형적 특징)")
        else:
            print("  ▶ 해석: 정규분포를 따른다고 볼 수 있습니다.")
    else:
        print("\n[2] 정규성 검정 실패")

    # 3. ADF 정상성 검정 (Stationarity Test) 결과 출력
    adf_res = tool.adf_test()
    if adf_res:
        print("\n[3] ADF 정상성 검정 (Augmented Dickey-Fuller Test)")
        print(f"  - 검정 통계량 (Statistics): {adf_res['statistics']:.4f}")
        print(f"  - 유의 확률 (p-value): {adf_res['p-value']:.4e}")
        print("  - 임계값 (Critical Values):")
        for key, value in adf_res['critical_values'].items():
            print(f"      {key}: {value:.4f}")
        
        if adf_res['is_stationary']:
            print("  ▶ 해석: 정상성(Stationarity)을 확보했습니다. (시계열 모델링 가능)")
        else:
            print("  ▶ 해석: 비정상(Non-stationary) 시계열입니다. (차분 등의 추가 전처리 필요)")
    else:
        print("\n[3] ADF 검정 실패")
        
    print("\n========================================================")
        
    
    
