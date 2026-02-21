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
        
        total = len(tickers_df)
        success_count = 0
        fail_count = 0
        start_time = time.time()

        print("=" * 60)
        print(f"🚀 데이터 파이프라인 시작: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"📦 대상 종목 수: {total}개 | 저장 모드: {'활성화' if save else '비활성화'}")
        print("=" * 60)

        for i, row in tickers_df.iterrows():
            ticker = str(row["code"]) # 타입을 문자열로 보장
            
            # [보완] 상장일 추출 및 결측치 처리
            listing_date = row.get('listing_date', None)
            if pd.isna(listing_date): # NaN 값인 경우 None으로 변경
                listing_date = None
            
            ticker_start = time.time()
            percentage = ((i + 1) / total) * 100
            
            # 진행 상황 표시 (엔터 없이 현재 행 유지)
            print(f"[{i+1}/{total}] {percentage:>5.1f}% | 처리 중: {ticker:<8}", end="\r")

            try:
                # 2. 분봉 업데이트 (이미 내부에서 datetime 인덱스로 변환됨)
                df_min = self.min_updater.get_updated_data(ticker, listing_date=listing_date, save=save)

                if df_min is not None and not df_min.empty:
                    # 3. 일봉 변환 및 저장 (datetime 인덱스를 인식하여 resample로 동작)
                    convert_to_daily(
                        df=df_min, 
                        ticker=ticker, 
                        save=save, 
                        save_dir=self.daily_save_dir
                    )
                    success_count += 1
                    status = "완료"
                    # 메모리 해제 지원
                    del df_min 
                else:
                    fail_count += 1
                    status = "데이터 없음"

            except Exception as e:
                fail_count += 1
                status = f"실패 ({str(e)[:20]}...)" # 에러 메시지 너무 길면 생략

            elapsed = time.time() - ticker_start
            # 최종 결과 한 줄 출력 (가독성을 위해 \r 제거 후 출력)
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
    # 전체 실행 전 테스트를 원하시면 tickers_df.head(10) 등으로 범위를 줄여 테스트하세요.
    pipeline.run_pipeline(save=True)