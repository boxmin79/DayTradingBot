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

for trade_file in trade_files:
    try:
        ticker = os.path.basename(trade_file).replace('trades_', '').replace('.parquet', '')
        chart_file = os.path.join(chart_dir, f"{ticker}.parquet")
        if not os.path.exists(chart_file): continue
            
        df_t = pd.read_parquet(trade_file)
        df_c = pd.read_parquet(chart_file)
        
        # 날짜 정규화 (시간 제거)
        t_col = next((c for c in ['entry_date', 'date', 'datetime'] if c in df_t.columns), None)
        c_col = next((c for c in ['date', 'datetime'] if c in df_c.columns), None)
        df_t['match_date'] = pd.to_datetime(df_t[t_col]).dt.normalize()
        df_c['match_date'] = pd.to_datetime(df_c[c_col]).dt.normalize()

        # 지표 계산
        df_c = df_c.sort_values('match_date')
        ma20 = df_c['close'].rolling(window=20).mean()
        df_c['slope20'] = ((ma20 / ma20.shift(1)) - 1) * 100
        df_c['disp20'] = ((df_c['close'] / ma20) - 1) * 100
            
        # 전날 지표 매칭
        df_ind = df_c[['match_date', 'slope20', 'disp20']].copy()
        df_ind[['slope20', 'disp20']] = df_ind[['slope20', 'disp20']].shift(1)
        
        merged = pd.merge(df_t, df_ind, on='match_date', how='inner')
        p_col = next((c for c in ['profit_rate', 'pnl', 'profit'] if c in merged.columns), None)
        merged['is_win'] = merged[p_col] > 0
        
        analysis_data.append(merged[['is_win', 'slope20', 'disp20']])
    except:
        continue

if not analysis_data:
    print("데이터를 찾을 수 없습니다.")
else:
    final_df = pd.concat(analysis_data, ignore_index=True).dropna()
    final_df['Outcome'] = final_df['is_win'].map({True: 'Win', False: 'Loss'})

    # 2. 산점도 시각화
    plt.figure(figsize=(12, 10))
    
    # 이상치 제외 (시각적 집중도를 위해 상하위 0.5% 제거)
    x_min, x_max = final_df['slope20'].quantile([0.005, 0.995])
    y_min, y_max = final_df['disp20'].quantile([0.005, 0.995])
    plot_df = final_df[final_df['slope20'].between(x_min, x_max) & final_df['disp20'].between(y_min, y_max)]

    # 점들이 겹칠 수 있으므로 투명도(alpha) 조절
    sns.scatterplot(data=plot_df, x='slope20', y='disp20', hue='Outcome', 
                    palette={'Win': 'blue', 'Loss': 'red'}, alpha=0.3, s=15)
    
    # 기준선 (0,0)
    plt.axvline(0, color='black', linestyle='--', linewidth=1)
    plt.axhline(0, color='black', linestyle='--', linewidth=1)
    
    plt.title('Scatter Analysis: 20-Day Slope vs Disparity', fontsize=15)
    plt.xlabel('20-Day MA Slope (%)', fontsize=12)
    plt.ylabel('20-Day MA Disparity (%)', fontsize=12)
    plt.grid(True, linestyle=':', alpha=0.6)
    plt.show()

    # 3. 사분면별 승률 분석
    q1 = final_df[(final_df['slope20'] > 0) & (final_df['disp20'] > 0)] # 우상단 (강세)
    q3 = final_df[(final_df['slope20'] < 0) & (final_df['disp20'] < 0)] # 좌하단 (약세)
    
    print(f"--- 사분면 승률 데이터 ---")
    if len(q1) > 0: print(f"1. 정배열 강세(Slope>0, Disp>0) 승률: {q1['is_win'].mean()*100:.2f}% ({len(q1)}건)")
    if len(q3) > 0: print(f"2. 역배열 약세(Slope<0, Disp<0) 승률: {q3['is_win'].mean()*100:.2f}% ({len(q3)}건)")