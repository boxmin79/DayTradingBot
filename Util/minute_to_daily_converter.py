import pandas as pd
import os
import sys

# 프로젝트 루트 경로 설정
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
MINUTE_DIR = os.path.join(BASE_DIR, "data", "chart", "minute")
DAILY_DIR = os.path.join(BASE_DIR, "data", "chart", "daily")

def convert_all_minute_to_daily():
    # 저장 폴더가 없으면 생성
    if not os.path.exists(DAILY_DIR):
        os.makedirs(DAILY_DIR)
        print(f"[알림] 일봉 저장 폴더 생성 완료: {DAILY_DIR}")

    # 분봉 폴더 내 CSV 파일 목록 가져오기
    files = [f for f in os.listdir(MINUTE_DIR) if f.endswith('.csv')]
    
    if not files:
        print(f"!!! [오류] 변환할 분봉 데이터가 없습니다. 경로를 확인하세요: {MINUTE_DIR}")
        return

    print(f"--- [시작] 총 {len(files)}개 종목 변환 시작 ---")

    for idx, file_name in enumerate(files):
        try:
            # 1. 분봉 데이터 로드
            file_path = os.path.join(MINUTE_DIR, file_name)
            df = pd.read_csv(file_path)
            
            if df.empty:
                continue

            # 2. 날짜 및 시간 설정 (기존 Collector 포맷: date, time)
            # datetime 컬럼 생성 (YYYYMMDD + HHMM)
            df['datetime'] = pd.to_datetime(
                df['date'].astype(str) + df['time'].astype(str).str.zfill(4), 
                format='%Y%m%d%H%M'
            )
            df.set_index('datetime', inplace=True)

            # 3. 일봉 리샘플링 (OHLCV)
            # 변동성 돌파 전략을 위해 'open', 'high', 'low', 'close', 'volume'을 정확히 추출
            daily_df = df.resample('1D').agg({
                'open': 'first',
                'high': 'max',
                'low': 'min',
                'close': 'last',
                'volume': 'sum'
            }).dropna() # 데이터가 없는 주말/공휴일 제거

            # 인덱스 이름을 'date'로 변경 (CSV 저장 시 헤더가 됨)
            daily_df.index.name = 'date'

            # 4. 결과 저장
            save_path = os.path.join(DAILY_DIR, file_name)
            daily_df.to_csv(save_path, encoding='utf-8-sig')
            
            # 진행 상황 출력
            if (idx + 1) % 100 == 0 or (idx + 1) == len(files):
                print(f"    [{idx + 1}/{len(files)}] 변환 중... ({file_name})")

            # 5. 메모리 해제 (32비트 환경 최적화)
            del df
            del daily_df

        except Exception as e:
            print(f"!!! [에러] {file_name} 변환 실패: {e}")

    print(f"--- [완료] 모든 파일이 {DAILY_DIR}에 저장되었습니다. ---")

if __name__ == "__main__":
    convert_all_minute_to_daily()