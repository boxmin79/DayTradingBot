import sys
import os
import pandas as pd
from PyQt5.QtWidgets import QApplication
from Strategy.volatility_breakout import VolatilityBreakout
from BackTest.volatility_backtest import VolatilityBacktest

def generate_investment_report(summary_df):
    """
    분석 데이터를 바탕으로 투자 유망 종목 리포트를 생성합니다.
    """
    if summary_df.empty: return

    # 문자열 수치를 연산 가능한 숫자로 변환
    df = summary_df.copy()
    df['ret_num'] = df['총수익률'].str.replace('%', '').astype(float)
    df['adj_ret_num'] = df['조정수익률'].str.replace('%', '').astype(float)
    df['mdd_num'] = df['MDD'].str.replace('%', '').astype(float)
    df['sqn_num'] = df['SQN'].astype(float)
    df['dist_num'] = df['매매분포지수'].astype(float)
    df['expect_num'] = df['기대값'].astype(float)

    # [투자 점수(Score) 산출 알고리즘]
    # 1. SQN 가중치 (안정성) - 40%
    # 2. 기대값 가중치 (수익 기대) - 30%
    # 3. 매매분포 (일관성) - 20% (낮을수록 좋음)
    # 4. MDD (리스크) - 10% (높을수록 좋음 - 마이너스 수치이므로)
    
    # 각 지표 표준화 (0~1 사이)
    def normalize(series, reverse=False):
        if reverse: return (series.max() - series) / (series.max() - series.min() + 1e-6)
        return (series - series.min()) / (series.max() - series.min() + 1e-6)

    df['score'] = (
        normalize(df['sqn_num']) * 0.4 +
        normalize(df['expect_num']) * 0.3 +
        normalize(df['dist_num'], reverse=True) * 0.2 +
        normalize(df['mdd_num']) * 0.1
    ) * 100

    # 필터링 조건: 매매 횟수가 최소 20회 이상이고 기대값이 양수인 종목
    recommended = df[(df['매매횟수'] >= 20) & (df['expect_num'] > 0)]
    recommended = recommended.sort_values(by='score', ascending=False).head(10)

    # 리포트 저장
    report_path = os.path.join(os.getcwd(), "data", "backtest", "top_picks_report.csv")
    recommended[['종목코드', '종목명', 'score', '총수익률', '조정수익률', 'SQN', '기대값', '매매분포지수', '매매횟수']].to_csv(report_path, index=False, encoding='utf-8-sig')
    
    return recommended, report_path

def main():
    app = QApplication(sys.argv)
    print("\n" + "="*70)
    print("   DayTradingBot 통합 백테스트 및 유망 종목 리포트 생성")
    print("="*70)

    strategy = VolatilityBreakout(k=0.5)
    backtester = VolatilityBacktest(strategy)
    
    ticker_path = os.path.join(os.getcwd(), "data", "ticker", "filtered_tickers.csv")
    summary_list = [] 

    if os.path.exists(ticker_path):
        df_tickers = pd.read_csv(ticker_path)
        for idx, row in df_tickers.iterrows():
            ticker_code = str(row['code']).zfill(6)
            pkl_path = os.path.join(os.getcwd(), "data", "chart", "minute", f"{ticker_code}.pkl")
            
            if os.path.exists(pkl_path):
                print(f"[*] [{idx+1}/{len(df_tickers)}] {row['name']} 분석 중...", end='\r')
                try:
                    df = pd.read_pickle(pkl_path)
                    _, metrics = backtester.run(df, ticker=ticker_code)
                    if metrics:
                        metrics['종목코드'] = ticker_code
                        metrics['종목명'] = row['name']
                        summary_list.append(metrics)
                except Exception: continue

        if summary_list:
            summary_df = pd.DataFrame(summary_list)
            # 전체 요약 저장
            summary_df.to_csv("data/backtest/total_summary.csv", index=False, encoding='utf-8-sig')
            
            # [리포트 생성 실행]
            recommended, path = generate_investment_report(summary_df)
            
            print("\n\n" + "★"*30)
            print("   최종 투자 유망 종목 TOP 5 (종합 점수 기준)")
            print("   (기준: 안정성 40%, 수익우위 30%, 분포 20%, 리스크 10%)")
            print("★"*30)
            print(recommended[['종목명', 'score', '총수익률', 'SQN', '기대값', '매매횟수']].head(5))
            print("-" * 70)
            print(f"==> 상세 리포트가 생성되었습니다: {path}")
    else:
        print("!!! filtered_tickers.csv 파일이 없습니다.")

if __name__ == "__main__":
    main()