import pandas as pd
import numpy as np
import os
import matplotlib.pyplot as plt
import pandas_ta as ta
from tqdm import tqdm
import warnings

warnings.filterwarnings('ignore')

class MinuteIndicatorAnalyzer:
    def __init__(self):
        self.report_path = os.path.join('data', 'backtest', 'volatility', 'summary', 'total_backtest_report.csv')
        self.result_dir = os.path.join('data', 'backtest', 'volatility', 'result')
        self.minute_dir = os.path.join('data', 'chart', 'minute') # 분봉 경로
        
        self.report = pd.read_csv(self.report_path)
        self.report['Ticker'] = self.report['Ticker'].astype(str).str.zfill(6)

    def calculate_minute_indicators(self, df):
        """진입 직전 분봉 데이터에 대한 특화 지표 계산"""
        # 1. 거래량 스파이크 (최근 5분 평균 / 60분 평균)
        v_ma5 = df['volume'].rolling(5).mean()
        v_ma60 = df['volume'].rolling(60).mean()
        df['vol_spike'] = v_ma5 / (v_ma60 + 1e-9)

        # 2. 가격 가속도 (Momentum) - 10분 전 대비 수익률
        df['mom_10'] = (df['close'] / df['close'].shift(10) - 1) * 100

        # 3. 당일 고가 대비 위치
        # 분봉 데이터에 'date' 또는 'time'이 있을 것이므로 당일 데이터를 추출해야 함
        # 여기서는 단순히 윈도우 내 최고가 대비 현재가 위치로 대체
        df['dist_high'] = (df['close'] / df['high'].rolling(60).max() - 1) * 100

        # 4. RSI (분봉상 과매수 여부)
        df['rsi_min'] = ta.rsi(df['close'], length=14)

        return df

    def get_minute_analysis(self, ticker):
        t_str = str(ticker).zfill(6)
        trade_path = os.path.join(self.result_dir, f"trades_{t_str}.parquet")
        minute_path = os.path.join(self.minute_dir, f"{t_str}.parquet")
        
        if not (os.path.exists(trade_path) and os.path.exists(minute_path)):
            return None
            
        try:
            trades_df = pd.read_parquet(trade_path)
            m_df = pd.read_parquet(minute_path)
            
            # 시간 컬럼 전처리 (데이터 형식에 따라 수정 필요)
            if 'date' in m_df.columns:
                m_df['date'] = pd.to_datetime(m_df['date']).dt.tz_localize(None)
                m_df.set_index('date', inplace=True)
            elif not isinstance(m_df.index, pd.DatetimeIndex):
                m_df.index = pd.to_datetime(m_df.index).dt.tz_localize(None)

            # 미리 전체 지표 계산 (매수 시점마다 자르는 것보다 빠름)
            m_df = self.calculate_minute_indicators(m_df)
            
            analysis_results = []
            for _, trade in trades_df.iterrows():
                # 매수 시점 (Timestamp)
                entry_dt = pd.to_datetime(trade['entry_date']).tz_localize(None)
                
                # 매수 시점 '직전' 1분봉 데이터 추출
                # 정확히 entry_dt에 찍힌 데이터는 해당 봉이 완성된 시점이므로 .iloc[-1] 사용
                prev_data = m_df[m_df.index <= entry_dt]
                if len(prev_data) < 10: continue
                
                ind = prev_data.iloc[-1]
                pnl_val = trade.get('return', trade.get('pnl', 0))
                
                analysis_results.append({
                    'is_win': float(pnl_val) > 0,
                    'vol_spike': float(ind.get('vol_spike', np.nan)),
                    'mom_10': float(ind.get('mom_10', np.nan)),
                    'dist_high': float(ind.get('dist_high', np.nan)),
                    'rsi_min': float(ind.get('rsi_min', np.nan))
                })
            return analysis_results
        except Exception as e:
            # print(f"Error analyzing {ticker}: {e}")
            return None

    def run_analysis(self):
        valid_tickers = self.report[self.report['Total Trades'] > 0]['Ticker'].unique()
        all_trade_data = []
        
        print(f"🚀 분봉 기반 전수 조사 시작 ({len(valid_tickers)}개 종목)...")
        for ticker in tqdm(valid_tickers):
            res = self.get_minute_analysis(ticker)
            if res: all_trade_data.extend(res)
                
        if not all_trade_data:
            print("데이터를 찾을 수 없습니다.")
            return

        df_res = pd.DataFrame(all_trade_data)
        
        # 결과 시각화
        metrics = [('vol_spike', 'Volume Spike (5/60)'), ('mom_10', '10min Momentum (%)'), 
                   ('dist_high', 'Dist from 60min High (%)'), ('rsi_min', 'Minute RSI')]
        
        fig, axes = plt.subplots(2, 2, figsize=(15, 12))
        axes = axes.flatten()
        
        for i, (col, title) in enumerate(metrics):
            clean_df = df_res.dropna(subset=[col])
            # 이상치 제거 (그래프 가독성 위해 상하위 1% 제거)
            q_low, q_high = clean_df[col].quantile(0.01), clean_df[col].quantile(0.99)
            clean_df = clean_df[(clean_df[col] > q_low) & (clean_df[col] < q_high)]
            
            win_data = clean_df[clean_df['is_win'] == True][col].values
            loss_data = clean_df[clean_df['is_win'] == False][col].values
            
            axes[i].hist(win_data, bins=40, alpha=0.5, label='Win', color='blue', density=True)
            axes[i].hist(loss_data, bins=40, alpha=0.5, label='Loss', color='red', density=True)
            axes[i].set_title(title, fontweight='bold')
            axes[i].legend()
            
        plt.tight_layout()
        plt.savefig('minute_indicator_analysis.png')
        print("✅ 분봉 분석 완료. minute_indicator_analysis.png 확인 요망.")

if __name__ == "__main__":
    analyzer = MinuteIndicatorAnalyzer()
    analyzer.run_analysis()