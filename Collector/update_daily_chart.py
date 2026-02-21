import pandas as pd
import os

def convert_to_daily(df: pd.DataFrame, ticker: str = None, save: bool = False, save_dir: str = None) -> pd.DataFrame:
    """
    분봉 -> 일봉 변환 및 선택적 저장 함수 (datetime 형식 지원)
    
    :param df: 분봉 데이터프레임 (datetime 인덱스 권장)
    :param ticker: 종목코드
    :param save: 저장 여부
    :param save_dir: 저장 경로
    """
    if df.empty:
        print("[!] 데이터가 비어 있어 변환을 중단합니다.")
        return pd.DataFrame()

    # 1. 인덱스/컬럼 형태에 따른 처리
    if isinstance(df.index, pd.DatetimeIndex):
        # [권장] datetime 인덱스인 경우 resample 사용 (매우 빠름)
        daily_df = df.resample('D').agg({
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'volume': 'sum'
        }).dropna()  # 주말/공휴일 등 데이터 없는 날짜 제거
        
        # 분석 편의를 위해 인덱스를 'date' 컬럼으로 리셋 (타입은 datetime64)
        daily_df = daily_df.reset_index().rename(columns={'datetime': 'date'})
        
    else:
        # 기존처럼 'date' 컬럼(int)이 있는 경우
        if 'date' in df.columns:
            # 정수형(YYYYMMDD)을 datetime 타입으로 변환
            df['date'] = pd.to_datetime(df['date'].astype(str), format='%Y%m%d')
        
        # 정렬 (Open/Close 정확도 보장)
        sort_cols = [c for c in ['date', 'time'] if c in df.columns]
        if sort_cols:
            df = df.sort_values(sort_cols)

        # 그룹화 연산 (date가 datetime 타입이므로 결과도 datetime 유지)
        daily_df = df.groupby('date', as_index=False).agg({
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'volume': 'sum'
        })

    # 2. 결과 정렬 및 타입 확인
    daily_df = daily_df.sort_values('date').reset_index(drop=True)
    # daily_df['date']는 이제 datetime64[ns] 타입입니다.

    # 3. 저장 로직
    if save:
        if not ticker or not save_dir:
            print("[!] 저장 실패: ticker와 save_dir 인자가 필요합니다.")
        else:
            os.makedirs(save_dir, exist_ok=True)
            file_name = ticker if not ticker.startswith('A') else ticker[1:]
            file_path = os.path.join(save_dir, f"{file_name}.parquet")
            
            # datetime 타입을 지원하는 fastparquet 또는 pyarrow 엔진 사용
            daily_df.to_parquet(file_path, engine='fastparquet', compression='snappy', index=True)
            print(f"[✔] {ticker}: 일봉 저장 완료 (date 컬럼 타입: datetime) -> {file_path}")

    return daily_df

# --- 테스트 코드 ---
if __name__ == "__main__":
    ticker = "005930"
    ticker = ticker if ticker.startswith('A') else "A" + ticker
    
    file_name = ticker[1:]
    # 파일 경로 예시 (실제 경로에 맞춰 수정 필요)
    try:
        df = pd.read_parquet(f"data/chart/minute/{file_name}.parquet")
        daily_df = convert_to_daily(df, ticker, save=True, save_dir="data/chart/daily")
        print("\n--- 변환된 일봉 데이터 정보 ---")
        print(daily_df.info())  # date 컬럼이 datetime64[ns]인지 확인
        print(daily_df.head())
    except FileNotFoundError:
        print(f"[!] {file_name}.parquet 파일을 찾을 수 없습니다.")