import pandas as pd
import os
import sys
from datetime import datetime

# 프로젝트 루트 경로 추가
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(BASE_DIR)

class CandidateFilter:
    def __init__(self):
        # 1. 입력 및 출력 경로 설정
        # 입력: 기존 성과 분석 결과 파일 경로
        self.file_path = os.path.join(BASE_DIR, "data", "backtest", "result", "volatilitybreakout", "summary_report", "top_tier_candidates.csv")
        # 출력: 요청하신 data/ticker/ 폴더로 변경
        self.output_dir = os.path.join(BASE_DIR, "data", "ticker")
        self.output_path = os.path.join(self.output_dir, "monitoring_tickers.csv")
        
        # 출력 디렉토리가 없으면 생성
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)

    def get_refined_candidates(self, min_sharpe=3.0, min_expectancy=0.5, min_win_rate=60.0):
        """
        성과 지표와 최근 시장 적응력(이번달/지난달 수익)을 기준으로 정예 종목을 필터링합니다.
        """
        if not os.path.exists(self.file_path):
            print(f"[!] 성과 분석 파일이 없습니다: {self.file_path}")
            return pd.DataFrame()

        df = pd.read_csv(self.file_path)

        # 2. 오늘 날짜 기준 동적 컬럼명 생성 (예: 2026-02 (%))
        now = datetime.now()
        curr_month_col = now.strftime('%Y-%m (%%)')
        
        # 지난 달 계산 로직
        if now.month == 1:
            prev_month_date = datetime(now.year - 1, 12, 1)
        else:
            prev_month_date = datetime(now.year, now.month - 1, 1)
        prev_month_col = prev_month_date.strftime('%Y-%m (%%)')

        # 3. 기본 성과 조건 필터 (안정성, 수익성, 확률)
        base_condition = (
            (df['Sharpe Ratio'] >= min_sharpe) &
            (df['Expectancy (%)'] >= min_expectancy) &
            (df['Win Rate (%)'] >= min_win_rate)
        )

        # 4. 최근 시장 적응력 필터 (이번 달 또는 지난 달 수익 발생)
        recent_conditions = []
        if curr_month_col in df.columns:
            recent_conditions.append(df[curr_month_col].fillna(-1) >= 0)
        if prev_month_col in df.columns:
            recent_conditions.append(df[prev_month_col].fillna(-1) >= 0)

        if recent_conditions:
            recent_match = pd.concat(recent_conditions, axis=1).any(axis=1)
        else:
            # 해당 컬럼이 아예 없는 월초 등에는 기본 조건만 적용
            recent_match = pd.Series([True] * len(df))

        # 5. 최종 필터 적용 및 정렬
        final_df = df[base_condition & recent_match].copy()
        final_df = final_df.sort_values(by='Sharpe Ratio', ascending=False)
        
        return final_df

    def save_monitoring_csv(self, df):
        """
        필터링된 종목의 전체 데이터를 CSV로 저장합니다.
        """
        if df.empty:
            print("[!] 필터링된 종목이 없습니다.")
            return

        # 티커 6자리 문자열 포맷팅 유지
        df['Ticker'] = df['Ticker'].apply(lambda x: str(x).zfill(6))
        
        # CSV 파일로 저장 (한글 깨짐 방지 위해 utf-8-sig 사용)
        df.to_csv(self.output_path, index=False, encoding='utf-8-sig')
        
        print(f"[✔] 총 {len(df)}개 정예 종목 데이터가 저장되었습니다.")
        print(f"[✔] 저장 위치: {self.output_path}")

if __name__ == "__main__":
    cf = CandidateFilter()
    refined_stocks = cf.get_refined_candidates()
    
    if not refined_stocks.empty:
        cf.save_monitoring_csv(refined_stocks)