import os
import sys
import pandas as pd
import time
from datetime import datetime

# 프로젝트 루트 경로 추가
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(BASE_DIR)

# 수집기 및 변환기 임포트 
from Collector.update_minute_chart import MinuteChartUpdater
from Collector.update_daily_chart import convert_to_daily

class DataPipeline:
    def __init__(self):
        self.ticker_path = os.path.join(BASE_DIR, "data", "ticker", "filtered_tickers.parquet")
        self.daily_save_dir = os.path.join(BASE_DIR, "data", "chart", "daily")
        self.min_updater = MinuteChartUpdater()

    def run_pipeline(self, save=True):
        """
        데이터 수집 및 변환 파이프라인 실행
        """
        # 1. 티커 리스트 로드
        if not os.path.exists(self.ticker_path):
            print(f"[!] 오류: 티커 파일을 찾을 수 없습니다 -> {self.ticker_path}")
            return

        tickers_df = pd.read_parquet(self.ticker_path)
        
        # [수정] 티커와 상장일을 함께 순회하기 위해 DataFrame 자체를 활용
        # 컬럼명 호환성 체크 (ticker vs code)
        code_col = 'ticker' if 'ticker' in tickers_df.columns else 'code'
        
        total = len(tickers_df)
        success_count = 0
        fail_count = 0
        start_time = time.time()

        print("=" * 60)
        print(f"🚀 데이터 파이프라인 시작: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"📦 대상 종목 수: {total}개 | 저장 모드: {'활성화' if save else '비활성화'}")
        print("=" * 60)

        # iterrows()를 사용하여 각 행의 정보(상장일 등) 접근
        for i, row in tickers_df.iterrows():
            ticker = row[code_col]
            # [추가] 상장일 추출 (없으면 None)
            listing_date = row.get('listing_date', None)
            
            ticker_start = time.time()
            percentage = ((i + 1) / total) * 100
            
            print(f"[{i+1}/{total}] {percentage:>5.1f}% | 현재 종목: {ticker}", end="\r")

            try:
                # 2. 분봉 업데이트 (상장일 인자 전달 추가)
                df_min = self.min_updater.get_updated_data(ticker, listing_date=listing_date, save=save)

                if df_min is not None and not df_min.empty:
                    # 3. 일봉 변환 및 저장
                    convert_to_daily(
                        df=df_min, 
                        ticker=ticker, 
                        save=save, 
                        save_dir=self.daily_save_dir
                    )
                    success_count += 1
                    status = "완료"
                else:
                    fail_count += 1
                    status = "데이터 없음"

            except Exception as e:
                fail_count += 1
                status = f"실패 ({e})"

            elapsed = time.time() - ticker_start
            # 줄바꿈 처리를 명확하게 하여 로그 가독성 확보
            print(f"[{i+1}/{total}] {percentage:>5.1f}% | {ticker:<8} | {status:<15} | 소요: {elapsed:.2f}초")

        # 최종 요약 출력
        total_elapsed = time.time() - start_time
        avg_time = total_elapsed / total if total > 0 else 0

        print("=" * 60)
        print(f"🏁 파이프라인 종료: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"✅ 성공: {success_count} | ❌ 실패: {fail_count} | ⏱ 총 소요시간: {total_elapsed/60:.1f}분")
        print(f"📊 평균 종목당 소요시간: {avg_time:.2f}초")
        print("=" * 60)

if __name__ == "__main__":
    pipeline = DataPipeline()
    pipeline.run_pipeline(save=True)