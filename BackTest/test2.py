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

print(f"총 {len(trade_files)}개의 파일을 분석합니다...")

for trade_file in trade_files:
    try:
        ticker = os.path.basename(trade_file).replace('trades_', '').replace('.parquet', '')
        chart_file = os.path.join(chart_dir, f"{ticker}.parquet")
        
        if not os.path.exists(chart_file): continue
            
        df_t = pd.read_parquet(trade_file)
        df_c = pd.read_parquet(chart_file)
        
        if df_t.empty or df_c.empty: continue

        # 날짜 컬럼 표준화 및 시간 제거
        t_date_col = next((c for c in ['entry_date', 'date', 'datetime'] if c in df_t.columns), None)
        c_date_col = next((c for c in ['date', 'datetime'] if c in df_c.columns), None)
        if not t_date_col or not c_date_col: continue

        df_t['match_date'] = pd.to_datetime(df_t[t_date_col]).dt.normalize()
        df_c['match_date'] = pd.to_datetime(df_c[c_date_col]).dt.normalize()

        # 2. 지표 계산 (이평선, 기울기, 이격도)
        df_c = df_c.sort_values('match_date')
        for w in [5, 10, 20]:
            ma = df_c['close'].rolling(window=w).mean()
            # 기울기 (%)
            df_c[f'slope{w}'] = ((ma / ma.shift(1)) - 1) * 100
            # 이격도 (%): (현재가 / 이평선 - 1) * 100
            df_c[f'disp{w}'] = ((df_c['close'] / ma) - 1) * 100
            
        # 진입 전날의 지표를 매칭하기 위해 shift(1)
        indicator_cols = ['match_date', 'slope5', 'slope10', 'slope20', 'disp5', 'disp10', 'disp20']
        df_ind = df_c[indicator_cols].copy()
        df_ind.iloc[:, 1:] = df_ind.iloc[:, 1:].shift(1)
        
        # 3. 데이터 병합
        merged = pd.merge(df_t, df_ind, on='match_date', how='inner')
        if merged.empty: continue

        # 수익 여부 판단
        p_col = next((c for c in ['profit_rate', 'pnl', 'profit'] if c in merged.columns), None)
        merged['is_win'] = merged[p_col] > 0
        
        analysis_data.append(merged[['is_win', 'slope5', 'slope10', 'slope20', 'disp5', 'disp10', 'disp20']])
        
    except Exception:
        continue

if not analysis_data:
    print("매칭된 데이터가 없습니다. 날짜 포맷이나 경로를 확인해주세요.")
else:
    final_df = pd.concat(analysis_data, ignore_index=True).dropna()
    final_df['is_win'] = final_df['is_win'].map({True: 'Win', False: 'Loss'})

    # 4. 시각화 (2행 3열 구조)
    fig, axes = plt.subplots(2, 3, figsize=(20, 10))
    metrics = [
        ('slope5', 'Slope 5D (%)'), ('slope10', 'Slope 10D (%)'), ('slope20', 'Slope 20D (%)'),
        ('disp5', 'Disparity 5D (%)'), ('disp10', 'Disparity 10D (%)'), ('disp20', 'Disparity 20D (%)')
    ]
    
    for i, (col, title) in enumerate(metrics):
        ax = axes[i // 3, i % 3]
        sns.histplot(data=final_df, x=col, hue='is_win', kde=True, element='step',
                     palette={'Win': 'blue', 'Loss': 'red'}, ax=ax, common_norm=False, stat='density')
        ax.set_title(title, fontweight='bold')
        # 이상치 제거 (1~99% 범위)
        ax.set_xlim(final_df[col].quantile(0.01), final_df[col].quantile(0.99))

    plt.suptitle('MA Slope & Disparity Analysis (Previous Day)', fontsize=16)
    plt.tight_layout(rect=[0, 0.03, 1, 0.95])
    plt.show()

    print("\n--- [평균 통계 요약] ---")
    print(final_df.groupby('is_win').mean())