import pandas as pd
import os

def convert_to_daily(df: pd.DataFrame, ticker: str = None, save: bool = False, save_dir: str = None) -> pd.DataFrame:
    """
    분봉 -> 일봉 초고속 변환 및 선택적 저장 함수
    
    :param df: 분봉 데이터프레임
    :param ticker: 종목코드 (저장 시 파일명으로 사용)
    :param save: True일 경우 지정된 경로에 parquet로 저장
    :param save_dir: 저장할 디렉토리 경로
    """
    if df.empty:
        print("[!] 데이터가 비어 있어 변환을 중단합니다.")
        return pd.DataFrame()

    # 1. 데이터 정렬 (Open/Close 정확도 보장)
    if 'time' in df.columns:
        df = df.sort_values(['date', 'time'])

    # 2. 속도 최적화된 그룹화 연산
    daily_df = df.groupby('date', as_index=False).agg({
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'volume': 'sum'
    })

    # 3. 결과 정렬 및 타입 확정
    daily_df = daily_df.sort_values('date').reset_index(drop=True)
    daily_df['date'] = daily_df['date'].astype(int)

    # 4. 저장 로직
    if save:
        if not ticker or not save_dir:
            print("[!] 저장 실패: ticker와 save_dir 인자가 필요합니다.")
        else:
            os.makedirs(save_dir, exist_ok=True)
            # 'A' 접두사 제거 후 파일명 생성
            file_name = ticker if not ticker.startswith('A') else ticker[1:]
            file_path = os.path.join(save_dir, f"{file_name}.parquet")
            
            # fastparquet 엔진으로 저장
            daily_df.to_parquet(file_path, engine='fastparquet', compression='snappy', index=False)
            print(f"[✔] {ticker}: 일봉 저장 완료 -> {file_path}")

    return daily_df

# --- 테스트 코드 ---
if __name__ == "__main__":
    # 티커 입력(삼성전자)
    ticker = "005930"
    ticker = ticker if ticker.startswith('A') else "A" + ticker
    
    # 삼성전자 parquet 파일 로드
    file_name = ticker[1:]
    df = pd.read_parquet(f"data/chart/minute/{file_name}.parquet")
    daily_df = convert_to_daily(df, ticker, save=True, save_dir="data/chart/daily")
    print(daily_df)
