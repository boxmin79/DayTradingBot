import pandas as pd
import os
import glob

# 1. 경로 설정
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.dirname(CURRENT_DIR) 

# 백테스트 결과가 있는 원본 경로
RESULT_DIR = os.path.join(BASE_DIR, "data", "backtest", "result", "volatilitybreakout")

# 2. 결과 저장 경로 설정 (사용자 요청 폴더)
SAVE_DIR = os.path.join(BASE_DIR, "data", "backtest", "result", "volatilitybreakout", "indicator_profit_analysis")
# 종목별 데이터를 저장할 하위 폴더
# TICKER_DATA_DIR = os.path.join(SAVE_DIR)

def analyze_and_save_all():
    # 저장 폴더 생성
    if not os.path.exists(SAVE_DIR):
        os.makedirs(SAVE_DIR)
        print(f"[*] 저장 폴더 준비 완료: {SAVE_DIR}")

    # 3. .parquet 파일 목록 가져오기
    files = glob.glob(os.path.join(RESULT_DIR, "*.parquet"))
    files = [f for f in files if "summary" not in os.path.basename(f)]

    if not files:
        print(f"[!] 분석할 데이터 파일이 없습니다: {RESULT_DIR}")
        return

    all_trades_list = []

    print(f"[*] 총 {len(files)}개 종목 분석 및 개별 데이터 저장 시작...")

    # 4. 파일별 순환 분석
    for f in files:
        ticker = os.path.basename(f).replace('.parquet', '')
        try:
            df = pd.read_parquet(f)
            if df.empty: continue
            
            all_trades_list.append(df)

            # --- [종목별 데이터 저장 로직] ---
            indicator_cols = [c for c in df.columns if c.startswith('m_') or c.startswith('d_')]
            if not indicator_cols: continue

            p_trades = df[df['profit'] > 0]
            l_trades = df[df['profit'] <= 0]

            ticker_stats = []
            for col in indicator_cols:
                ticker_stats.append({
                    'Indicator': col,
                    'Profit_Mean': p_trades[col].mean(),
                    'Loss_Mean': l_trades[col].mean(),
                    'Diff': p_trades[col].mean() - l_trades[col].mean()
                })
            
            # 종목별 통계 저장
            ticker_df = pd.DataFrame(ticker_stats)
            ticker_df.to_parquet(os.path.join(SAVE_DIR, f"{ticker}.parquet"), engine='fastparquet', compression='snappy', index=False)
            
        except Exception as e:
            print(f"[!] {ticker} 분석 실패: {e}")

    # 5. 전체 통합 데이터 분석 및 저장
    if all_trades_list:
        combined_df = pd.concat(all_trades_list, ignore_index=True)
        indicator_cols = [c for c in combined_df.columns if c.startswith('m_') or c.startswith('d_')]
        
        profit_trades = combined_df[combined_df['profit'] > 0]
        loss_trades = combined_df[combined_df['profit'] <= 0]

        total_results = []
        for col in indicator_cols:
            p_mean = profit_trades[col].mean()
            l_mean = loss_trades[col].mean()
            total_results.append({
                'Indicator': col,
                'Total_Profit_Mean': p_mean,
                'Total_Loss_Mean': l_mean,
                'Total_Diff': p_mean - l_mean
            })

        # 통합 리포트 저장
        final_df = pd.DataFrame(total_results)
        total_save_dir = os.path.join(SAVE_DIR, "total")
        os.makedirs(total_save_dir, exist_ok=True)  
        total_save_path = os.path.join(total_save_dir, "total_indicator_profit_analysis.csv")
        final_df.to_csv(total_save_path, index=False, encoding='utf-8-sig')
        
        print("\n" + "="*50)
        print(f"[✔] 통합 리포트 저장: {total_save_path}")
        print(f"[✔] 종목별 상세 데이터 저장 완료 (총 {len(files)}개 종목)")
        print("="*50)

if __name__ == "__main__":
    analyze_and_save_all()