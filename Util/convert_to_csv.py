import pandas as pd
import os

# 1. 파일 경로 설정
base_path = "data/backtest/result/volatilitybreakout"
parquet_file = os.path.join(base_path, "total_performance_summary.parquet")
csv_file = os.path.join(base_path, "total_performance_summary.csv")

def convert_parquet_to_csv():
    if not os.path.exists(parquet_file):
        print(f"[!] 파일을 찾을 수 없습니다: {parquet_file}")
        return

    print(f"[*] 변환 시작: {parquet_file}")

    try:
        # 2. 파일 로드 (환경에 따라 엔진을 선택적으로 사용)
        # 만약 fastparquet 버전 에러가 나면 engine='pyarrow'로 시도해보세요.
        try:
            df = pd.read_parquet(parquet_file, engine='fastparquet')
        except:
            df = pd.read_parquet(parquet_file, engine='pyarrow')

        # 3. CSV로 저장 
        # 한글 깨짐 방지를 위해 'utf-8-sig' 인코딩을 사용합니다.
        df.to_csv(csv_file, index=False, encoding='utf-8-sig')
        
        print(f"[+] 변환 완료! 파일 위치: {csv_file}")
        print(f"[*] 총 {len(df)}개 종목의 데이터가 저장되었습니다.")

    except Exception as e:
        print(f"[!] 에러 발생: {e}")
        print("팁: pip install \"fastparquet<0.9.0\" 명령어로 pandas 1.4.2용 엔진을 설치했는지 확인하세요.")

if __name__ == "__main__":
    convert_parquet_to_csv()