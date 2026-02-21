import pandas as pd
import numpy as np
import os
import matplotlib.pyplot as plt
import pandas_ta as ta
from tqdm import tqdm
import warnings

# 경고 메시지 무시 설정
warnings.filterwarnings('ignore', category=FutureWarning)

class IndicatorAnalyzer:
    def __init__(self):
        # 1. 경로 설정 (프로젝트 루트 기준)
        self.report_path = os.path.join('data', 'backtest', 'volatility', 'summary', 'total_backtest_report.csv')
        self.result_dir = os.path.join('data', 'backtest', 'volatility', 'result')
        self.daily_dir = os.path.join('data', 'chart', 'daily')
        
        if not os.path.exists(self.report_path):
            self.report_path = os.path.abspath(self.report_path)
            if not os.path.exists(self.report_path):
                raise FileNotFoundError(f"리포트 파일을 찾을 수 없습니다.\n경로: {self.report_path}")
            
        self.report = pd.read_csv(self.report_path)
        self.report['Ticker'] = self.report['Ticker'].astype(str).str.zfill(6)
        print(f"✅ 리포트 로드 완료: {len(self.report)}개 종목")

    def calculate_indicators(self, df):
        """기술적 지표 계산 (BB Width 로직 수정 및 RVI/Inertia 추가)"""
        
        # 1. RSI
        try: df['rsi'] = ta.rsi(df['close'], length=14)
        except: df['rsi'] = np.nan
            
        # 2. MACD
        try:
            macd = ta.macd(df['close'])
            if macd is not None:
                hist_col = [c for c in macd.columns if 'MACDh' in c]
                df['macd_h'] = macd[hist_col[0]] if hist_col else np.nan
        except: df['macd_h'] = np.nan
            
        # 3. ADX (추세 강도)
        try:
            adx_df = ta.adx(df['high'], df['low'], df['close'], length=14)
            df['adx'] = adx_df['ADX_14'] if adx_df is not None else np.nan
        except: df['adx'] = np.nan
            
        # 4. 볼린저 밴드 Width (동적 컬럼 매칭으로 수정)
        try:
            bb = ta.bbands(df['close'], length=20, std=2)
            if bb is not None:
                upper_col = [c for c in bb.columns if c.startswith('BBU')][0]
                lower_col = [c for c in bb.columns if c.startswith('BBL')][0]
                mid_col = [c for c in bb.columns if c.startswith('BBM')][0]
                df['bb_width'] = (bb[upper_col] - bb[lower_col]) / bb[mid_col]
        except: df['bb_width'] = np.nan
            
        # 5. MFI (자금 흐름)
        try: df['mfi'] = ta.mfi(df['high'], df['low'], df['close'], df['volume'], length=14)
        except: df['mfi'] = np.nan
            
        # 6. 노이즈 비율 (5일 평균)
        try:
            noise = 1 - abs(df['close'] - df['open']) / (df['high'] - df['low'] + 1e-9)
            df['noise_avg'] = noise.rolling(5).mean()
        except: df['noise_avg'] = np.nan

        # 7. RVI (Relative Vigor Index)
        try:
            rvi_df = ta.rvi(df['open'], df['high'], df['low'], df['close'], length=14)
            if rvi_df is not None:
                rvi_col = [c for c in rvi_df.columns if 'RVI_' in c and 's' not in c.lower()]
                df['rvi'] = rvi_df[rvi_col[0]] if rvi_col else np.nan
        except: df['rvi'] = np.nan

        # 8. Inertia (관성지수)
        try:
            # 관성지수는 내부적으로 RVI의 선형회귀를 사용하거나 데이터의 선형성을 측정함
            df['inertia'] = ta.inertia(df['close'], length=20)
        except: df['inertia'] = np.nan
            
        return df

    def get_trade_analysis(self, ticker):
        """매매 기록과 일봉 지표 결합"""
        t_str = str(ticker).zfill(6)
        trade_path = os.path.join(self.result_dir, f"trades_{t_str}.parquet")
        daily_path = os.path.join(self.daily_dir, f"{t_str}.parquet")
        
        if not (os.path.exists(trade_path) and os.path.exists(daily_path)):
            return None
            
        try:
            trades_df = pd.read_parquet(trade_path)
            if trades_df.empty: return None
            
            d_df = pd.read_parquet(daily_path)
            if 'date' in d_df.columns:
                d_df.set_index('date', inplace=True)
            
            d_df.index = pd.to_datetime(d_df.index).tz_localize(None).normalize()
            d_df = self.calculate_indicators(d_df).sort_index()
            
            analysis_results = []
            for _, trade in trades_df.iterrows():
                entry_dt = pd.to_datetime(trade['entry_date']).tz_localize(None).normalize()
                prev_data = d_df[d_df.index < entry_dt]
                if prev_data.empty: continue
                
                ind = prev_data.iloc[-1]
                pnl_val = trade.get('return', trade.get('pnl', 0))
                
                analysis_results.append({
                    'is_win': float(pnl_val) > 0,
                    'rsi': float(ind.get('rsi', np.nan)),
                    'macd_h': float(ind.get('macd_h', np.nan)),
                    'adx': float(ind.get('adx', np.nan)),
                    'bb_width': float(ind.get('bb_width', np.nan)),
                    'mfi': float(ind.get('mfi', np.nan)),
                    'noise': float(ind.get('noise_avg', np.nan)),
                    'rvi': float(ind.get('rvi', np.nan)),
                    'inertia': float(ind.get('inertia', np.nan))
                })
            return analysis_results
        except Exception:
            return None

    def print_win_rate_report(self, df):
        """지표별 구간 승률 출력"""
        print("\n" + "="*60)
        print("📊 지표별 주요 구간 승률 리포트")
        print("="*60)
        
        configs = [
            ('adx', [0, 20, 30, 100], ['Low (<20)', 'Mid (20-30)', 'High (>30)']),
            ('noise', [0, 0.4, 0.6, 1.0], ['Low (<0.4)', 'Normal (0.4-0.6)', 'High (>0.6)']),
            ('inertia', [0, 45, 55, 100], ['Bearish (<45)', 'Neutral (45-55)', 'Bullish (>55)']),
            ('bb_width', [0, 0.1, 0.2, 1.0], ['Squeeze (<0.1)', 'Normal (0.1-0.2)', 'Wide (>0.2)'])
        ]
        
        for col, bins, labels in configs:
            if col not in df.columns: continue
            valid_df = df.dropna(subset=[col]).copy()
            if valid_df.empty: continue

            valid_df[f'{col}_bin'] = pd.cut(valid_df[col], bins=bins, labels=labels)
            analysis = valid_df.groupby(f'{col}_bin', observed=False)['is_win'].agg(['count', 'mean'])
            analysis['Win Rate'] = (analysis['mean'] * 100).round(2).astype(str) + '%'
            print(f"\n[{col.upper()} 구간별 승률]")
            print(analysis[['count', 'Win Rate']])

    def run_analysis(self):
        valid_tickers = self.report[self.report['Total Trades'] > 0]['Ticker'].unique()
        
        all_trade_data = []
        print(f"🔍 총 {len(valid_tickers)}개 종목 분석 시작...")
        
        for ticker in tqdm(valid_tickers):
            res = self.get_trade_analysis(ticker)
            if res: all_trade_data.extend(res)
                
        if not all_trade_data:
            print("\n[!] 분석할 데이터가 없습니다.")
            return

        df_res = pd.DataFrame(all_trade_data)
        self.print_win_rate_report(df_res)
        
        # 4. 시각화 (3x3 레이아웃)
        metrics = [('adx', 'ADX'), ('noise', 'Noise Ratio'), ('mfi', 'MFI'), 
                   ('bb_width', 'BB Width'), ('rsi', 'RSI'), ('macd_h', 'MACD Hist'),
                   ('rvi', 'RVI'), ('inertia', 'Inertia')]
        
        fig, axes = plt.subplots(3, 3, figsize=(20, 15))
        axes = axes.flatten()
        
        for i, (col, title) in enumerate(metrics):
            if i >= len(axes): break
            if col not in df_res.columns: continue
            
            clean_df = df_res.dropna(subset=[col])
            win_data = clean_df[clean_df['is_win'] == True][col].values
            loss_data = clean_df[clean_df['is_win'] == False][col].values
            
            has_plot = False
            if len(win_data) > 0:
                axes[i].hist(win_data, bins=30, alpha=0.5, label='Win', color='blue', density=True)
                has_plot = True
            if len(loss_data) > 0:
                axes[i].hist(loss_data, bins=30, alpha=0.5, label='Loss', color='red', density=True)
                has_plot = True
            
            axes[i].set_title(title, fontsize=14, fontweight='bold')
            if has_plot:
                axes[i].legend()
        
        # 남은 빈 서브플롯 제거
        for j in range(len(metrics), len(axes)):
            fig.delaxes(axes[j])
            
        plt.tight_layout()
        plt.savefig('indicator_analysis_final_v2.png')
        print(f"\n✅ 분석 완료: 총 {len(df_res)}건 매매 분석됨. 결과 저장 완료.")

if __name__ == "__main__":
    analyzer = IndicatorAnalyzer()
    analyzer.run_analysis()