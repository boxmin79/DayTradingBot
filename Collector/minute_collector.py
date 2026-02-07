import os
import pandas as pd
from API.Daishin.stock_chart import DaishinStockChart

class MinuteCollector:
    def __init__(self):
        # 상속 구조를 갖춘 차트 모듈 (내부에 wait_for_limit 포함됨)
        self.api = DaishinStockChart()
        self.base_dir = os.getcwd()
        self.save_dir = os.path.join(self.base_dir, "data", "chart", "minute")
        self.ticker_path = os.path.join(self.base_dir, "data", "ticker", "filtered_tickers.csv")
        
        if not os.path.exists(self.save_dir):
            os.makedirs(self.save_dir)

    def collect_all_tickers(self):
        """filtered_tickers.csv의 모든 종목을 순회하며 분봉 수집"""
        if not os.path.exists(self.ticker_path):
            print(f"!!! [오류] 종목 리스트 파일이 없습니다: {self.ticker_path}")
            return

        # 1. 종목 리스트 로드
        df_tickers = pd.read_csv(self.ticker_path)
        total_count = len(df_tickers)
        print(f"[*] 총 {total_count}개 종목 수집을 시작합니다.")

        # 2. 전 종목 순회 루프
        for idx, row in df_tickers.iterrows():
            ticker_code = str(row['code']).zfill(6)
            ticker_name = row['name']
            ds_code = "A" + ticker_code  # 대신증권 코드 형식
            
            print(f"\n[{idx + 1}/{total_count}] {ticker_name}({ticker_code}) 작업 중...")
            
            try:
                # 내부 로직 호출 (target_count=200,000 고정)
                self._update_single_ticker(ds_code)
            except Exception as e:
                print(f"    !!! {ticker_name} 수집 중 에러 발생: {e}")
                continue # 에러 발생 시 다음 종목으로 진행

        print("\n" + "="*50)
        print("   전 종목 분봉 데이터 업데이트 완료")
        print("="*50)

    def _update_single_ticker(self, code, target_count=200000):
        """개별 종목 업데이트 핵심 로직 (내부용)"""
        file_path = os.path.join(self.save_dir, f"{code[1:]}.pkl")
        existing_df = pd.DataFrame()

        # 기존 데이터 로드
        if os.path.exists(file_path):
            existing_df = pd.read_pickle(file_path)

        all_new_data = []
        current_collected = 0

        # 수집 루프
        while current_collected < target_count:
            # api.py의 상속을 받은 stock_chart가 호출 시점에 Limit을 자동 관리함
            df = self.api.get_stock_chart(code, 2247, 'm', 1)
            
            if df is None or df.empty:
                break

            all_new_data.append(df)
            current_collected += len(df)
            
            # 업데이트 중단 로직 (중복 지점 확인)
            if not existing_df.empty:
                last_new_date = df.iloc[-1]['date']
                last_new_time = df.iloc[-1]['time']
                if last_new_date < existing_df.iloc[-1]['date'] or \
                   (last_new_date == existing_df.iloc[-1]['date'] and last_new_time <= existing_df.iloc[-1]['time']):
                    break

            if not self.api.obj_stock_chart.Continue:
                break

        # 데이터 통합 및 저장
        if all_new_data:
            new_df = pd.concat(all_new_data, ignore_index=True)
            combined_df = pd.concat([existing_df, new_df], ignore_index=True)
            combined_df.drop_duplicates(subset=['date', 'time'], keep='last', inplace=True)
            combined_df.sort_values(by=['date', 'time'], inplace=True)
            
            # 최신 20만개 유지
            if len(combined_df) > target_count:
                combined_df = combined_df.tail(target_count)
            
            combined_df.to_pickle(file_path)
            print(f"    => [저장 완료] 현재 {len(combined_df):,}행 보유")
        else:
            print(f"    => [최신 상태] 업데이트할 데이터가 없습니다.")

if __name__ == "__main__":
    MinuteCollector().collect_all_tickers()