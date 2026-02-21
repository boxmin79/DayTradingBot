import pandas as pd
import glob
import os
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np

# 1. 경로 설정
trade_dir = r'data\backtest\volatility\result'
chart_dir = r'data\chart\daily'

analysis_data = []
trade_files = glob.glob(os.path.join(trade_dir, 'trades_*.parquet'))

print(f"총 {len(trade_files)}개 파일을 분석하여 최적 구간을 탐색합니다...")

for trade_file in trade_files:
    try:
        ticker = os.path.basename(trade_file).replace('trades_', '').replace('.parquet', '')
        chart_file = os.path.join(chart_dir, f"{ticker}.parquet")
        if not os.path.exists(chart_file): continue
            
        df_t = pd.read_parquet(trade_file)
        df_c = pd.read_parquet(chart_file)
        
        # 날짜 전처리
        t_col = next((c for c in ['entry_date', 'date', 'datetime'] if c in df_t.columns), None)
        c_col = next((c for c in ['date', 'datetime'] if c in df_c.columns), None)
        df_t['match_date'] = pd.to_datetime(df_t[t_col]).dt.normalize()
        df_c['match_date'] = pd.to_datetime(df_c[c_col]).dt.normalize()

        # 5MA 지표 계산
        df_c = df_c.sort_values('match_date')
        ma5 = df_c['close'].rolling(window=5).mean()
        df_c['slope5'] = ((ma5 / ma5.shift(1)) - 1) * 100
        df_c['disp5'] = ((df_c['close'] / ma5) - 1) * 100
            
        # 진입 전날 지표 매칭 (Shift 1)
        df_ind = df_c[['match_date', 'slope5', 'disp5']].copy()
        df_ind[['slope5', 'disp5']] = df_ind[['slope5', 'disp5']].shift(1)
        
        merged = pd.merge(df_t, df_ind, on='match_date', how='inner')
        p_col = next((c for c in ['profit_rate', 'pnl', 'profit'] if c in merged.columns), None)
        
        # 승률 계산용 (Win=1, Loss=0)
        merged['is_win'] = (merged[p_col] > 0).astype(int)
        analysis_data.append(merged[['is_win', 'slope5', 'disp5']])
    except:
        continue

if not analysis_data:
    print("분석할 데이터를 찾지 못했습니다.")
else:
    df = pd.concat(analysis_data, ignore_index=True).dropna()
    
    # 2. 최적 구간 탐색을 위한 구간 나누기 (Binning)
    # 극단적인 이상치를 제외한 범위에서 10x10 격자를 만듭니다.
    s_min, s_max = df['slope5'].quantile([0.05, 0.95])
    d_min, d_max = df['disp5'].quantile([0.05, 0.95])
    
    # 10개 구간으로 등분
    df['slope_bin'] = pd.cut(df['slope5'], bins=np.linspace(s_min, s_max, 11))
    df['disp_bin'] = pd.cut(df['disp5'], bins=np.linspace(d_min, d_max, 11))
    
    # 3. 구간별 승률 데이터 생성 (Pivot Table)
    # mean은 승률, count는 해당 구간의 거래 횟수
    pivot = df.pivot_table(index='disp_bin', columns='slope_bin', values='is_win', aggfunc=['mean', 'count'])
    
    # 4. 히트맵 시각화
    plt.figure(figsize=(15, 10))
    win_rate = pivot['mean'] * 100
    
    # 거래 횟수가 너무 적은 구간(전체의 0.5% 미만)은 신뢰도가 낮으므로 마스킹 처리할 수 있음
    sns.heatmap(win_rate, annot=True, fmt=".1f", cmap='RdYlBu', center=df['is_win'].mean()*100)
    
    plt.title('Optimization: Win Rate (%) by 5MA Slope & Disparity', fontsize=16)
    plt.xlabel('5-Day MA Slope (%)', fontsize=12)
    plt.ylabel('5-Day MA Disparity (%)', fontsize=12)
    plt.gca().invert_yaxis() # 높은 이격도가 위로 가게 설정
    plt.tight_layout()
    plt.show()

    # 5. 최적값 상위 5개 구간 출력 (최소 거래 건수 보장)
    min_count = len(df) * 0.01 # 최소 전체 거래의 1% 이상인 구간만
    valid_zones = win_rate[pivot['count'] >= min_count].stack().sort_values(ascending=False).head(5)
    
    print("\n" + "="*50)
    print(f"분석된 총 거래 건수: {len(df)}건")
    print(f"전체 평균 승률: {df['is_win'].mean()*100:.2f}%")
    print("="*50)
    print("--- [검증된 최적의 5MA 구간 TOP 5] ---")
    for (disp, slope), wr in valid_zones.items():
        print(f"승률: {wr:.2f}% | 기울기 구간: {slope} | 이격도 구간: {disp}")