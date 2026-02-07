import pandas as pd
import numpy as np
import os
from .backtest import Backtest

class VolatilityBacktest(Backtest):
    """
    변동성 돌파 전략 전용 백테스터
    - 에러 수정: 데이터 로드 시 인덱스가 날짜가 아닌 경우 자동 변환
    - 기능: 슬리피지(갭상승) 적용 수익률과 이론 수익률 동시 계산
    """
    def __init__(self, strategy):
        super().__init__(strategy)

    def run(self, df: pd.DataFrame, ticker=None, fee=0.0015): 
        # ===========================================================
        # [에러 수정 파트] 인덱스 날짜 변환 로직
        # ===========================================================
        # 인덱스가 이미 날짜 형식이면 건너뜀
        if not isinstance(df.index, pd.DatetimeIndex):
            try:
                # 1. 'date'와 'time' 컬럼이 모두 있는 경우 (분봉 데이터)
                if 'date' in df.columns and 'time' in df.columns:
                    # 예: 20250101(숫자) + 900(숫자) -> "202501010900" 문자열로 변환
                    date_str = df['date'].astype(str)
                    time_str = df['time'].astype(str).str.zfill(4) # 900 -> 0900으로 채움
                    
                    # 날짜+시간 문자열 합치기
                    datetime_str = date_str + time_str
                    
                    # 인덱스로 설정
                    df.index = pd.to_datetime(datetime_str, format='%Y%m%d%H%M')
                
                # 2. 'date' 컬럼만 있는 경우 (일봉 데이터 등)
                elif 'date' in df.columns:
                    df.index = pd.to_datetime(df['date'].astype(str))
                    
            except Exception as e:
                print(f"  -> [경고] 인덱스 변환 중 오류 발생: {e}")

        # 변환 후에도 인덱스가 날짜가 아니면 실행 불가 (빈 결과 반환)
        if not isinstance(df.index, pd.DatetimeIndex):
            print(f"  -> [에러] 날짜 인덱스를 생성할 수 없습니다. (데이터 형식 확인 필요)")
            return pd.DataFrame(), {}
        # ===========================================================
        
        # 1. 전략 지표 계산
        df = self.strategy.apply_strategy(df)
        
        daily_records = []
        
        # 2. 날짜별 매매 시뮬레이션
        # 이제 df.index가 날짜 형식이므로 .date 속성 사용 가능
        grouped = df.groupby(df.index.date)
        
        for date, day_df in grouped:
            # 목표가 유효성 체크
            if day_df['target_price'].isna().all():
                continue

            target_price = day_df['target_price'].iloc[0]
            
            # 목표가가 0 이하인 경우 제외
            if pd.isna(target_price) or target_price <= 0:
                continue

            # 돌파 여부 확인
            breakout = day_df[day_df['high'] >= target_price]
            
            if not breakout.empty:
                first_candle = breakout.iloc[0]
                
                # A. 슬리피지 적용 (갭상승 고려: 시가가 목표가보다 높으면 시가 체결)
                real_buy_price = max(target_price, first_candle['open'])
                
                # B. 이론가 (무조건 목표가 체결 가정)
                theoretical_buy_price = target_price
                
                # 청산 (종가)
                sell_price = day_df['close'].iloc[-1]
                
                # 수익률 계산 (매수/매도 수수료 차감)
                real_return = (sell_price - real_buy_price) / real_buy_price - (fee * 2)
                theoretical_return = (sell_price - theoretical_buy_price) / theoretical_buy_price - (fee * 2)
                
                slippage_cost = theoretical_return - real_return
                
            else:
                real_return = 0.0
                theoretical_return = 0.0
                slippage_cost = 0.0
                real_buy_price = 0.0
                sell_price = 0.0
                
            daily_records.append({
                'date': date,
                'buy_price': real_buy_price,
                'sell_price': sell_price,
                'return': real_return,
                'return_no_slip': theoretical_return,
                'slippage': slippage_cost
            })
            
        # 3. 결과 정리
        result_df = pd.DataFrame(daily_records)
        
        if result_df.empty:
            return pd.DataFrame(), {"total_return": 0.0, "win_rate": 0.0, "mdd": 0.0}

        result_df.set_index('date', inplace=True)
        
        # 4. 성과 지표 계산
        try:
            metrics = self.calculate_metrics(result_df)
            
            # 이론 수익률 추가 지표
            result_df['cum_return_no_slip'] = (1 + result_df['return_no_slip']).cumprod()
            theoretical_total_return = (result_df['cum_return_no_slip'].iloc[-1] - 1) * 100
            metrics['total_return_no_slip'] = round(theoretical_total_return, 2)
            
        except AttributeError:
            # 부모 클래스 메서드 부재 시 비상용 로직
            result_df['cum_return'] = (1 + result_df['return']).cumprod()
            total_return = (result_df['cum_return'].iloc[-1] - 1) * 100
            metrics = {"total_return": round(total_return, 2)}
        
        # 5. CSV 파일 저장 (data/backtest 폴더)
        if ticker is not None:
            save_dir = os.path.join("data", "backtest")
            if not os.path.exists(save_dir):
                os.makedirs(save_dir)
            
            strategy_name = self.strategy.name if hasattr(self.strategy, 'name') else "VolatilityBreakout"
            file_name = f"backtest_{ticker}_{strategy_name}.csv"
            save_path = os.path.join(save_dir, file_name)
            
            try:
                result_df.to_csv(save_path)
            except Exception as e:
                print(f"  -> [경고] 결과 파일 저장 실패: {e}")

        return result_df, metrics