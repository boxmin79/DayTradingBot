import os
import sys

# 1. 프로젝트 루트 경로 자동 등록
# 현재 파일(BackTest/volatility_backtest.py)의 부모 디렉토리를 찾아 BASE_DIR로 설정합니다.
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.dirname(CURRENT_DIR)

if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

# 2. 필수 모듈 임포트
import pandas as pd
from Collector.add_indicator import ChartIndicatorAdder # Collector 패키지 내 클래스

class VolatilityBacktest:
    def __init__(self, strategy, base_dir="data/backtest/result"):
        self.strategy = strategy
        self.strategy_name = self.strategy.name
        self.base_dir = base_dir
        self.output_dir = os.path.join(base_dir, self.strategy_name)
        self.total_summary_logs = []
        self.indicator_adder = ChartIndicatorAdder()
        
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir, exist_ok=True)

    def run(self, ticker, minute_df, daily_df, save=False):
        """
        개별 종목에 대한 백테스트 실행 및 결과 저장 여부 결정
        """
        try:
            # 1. 지표 추가 (작성하신 ChartIndicatorAdder 활용)
            # 분봉과 일봉 데이터에 모든 기술적 지표를 컬럼으로 추가합니다.
            minute_df = self.indicator_adder.add_indicators(minute_df)
            daily_df = self.indicator_adder.add_indicators(daily_df)
            
            # 데이터가 부족하여 지표 계산에 실패한 경우 중단합니다.
            if minute_df.empty or daily_df.empty:
                return False

            # 2. 일봉에서 당일 적용할 목표가(target_price) 계산
            # 전일(shift 1) 데이터를 활용해 오늘 아침 확정된 목표가를 산출합니다.
            daily_df['range'] = daily_df['high'].shift(1) - daily_df['low'].shift(1)
            daily_df['target_price'] = daily_df['open'] + (daily_df['range'] * self.strategy.k)
            
            valid_daily = daily_df.dropna(subset=['target_price'])

            # 3. 고속 시뮬레이션 실행 (지표가 포함된 데이터 사용)
            ticker_trade_logs = self._simulate_efficient(ticker, minute_df, valid_daily)

            # 4. 결과 요약 및 저장
            if ticker_trade_logs:
                self._add_to_summary(ticker, ticker_trade_logs)
                
                # [수정된 부분] save=True일 경우 상세 내역을 Parquet으로 저장
                if save:
                    res_df = pd.DataFrame(ticker_trade_logs)
                    # 파일 확장자를 .parquet으로 변경합니다.
                    save_path = os.path.join(self.output_dir, f"{ticker}.parquet")
                    
                    # fastparquet 또는 pyarrow 엔진을 사용하여 저장합니다.
                    res_df.to_parquet(save_path, engine='fastparquet', compression='snappy', index=False)
                    # print(f"[*] {ticker}: 상세 매매 내역 저장 완료 ({save_path})") # 디버깅용
                    
                return True
                
        except Exception as e:
            print(f"[{ticker}] 백테스트 중 에러 발생: {e}")
        return False

    def _simulate_efficient(self, ticker, min_df, day_df):
        logs = []
        # 일봉의 모든 지표를 날짜별로 매핑 (딕셔너리화)
        day_dict = day_df.set_index('date').to_dict('index')
        
        # 비용 설정 (수수료+세금+슬리피지 합계 약 0.3%)
        total_cost_ratio = 0.003 
        stop_loss_rate = getattr(self.strategy, 'stop_loss_rate', 0.02)

        target_dict = {d: v['target_price'] for d, v in day_dict.items()}
        close_dict = {d: v['close'] for d, v in day_dict.items()}

        min_df['target_price'] = min_df['date'].map(target_dict)
        active_df = min_df.dropna(subset=['target_price']).copy()
        
        # 목표가 돌파 시점 확인
        active_df['is_breakout'] = active_df['high'] >= active_df['target_price']
        breakout_days = active_df[active_df['is_breakout']].groupby('date')

        for date, entries in breakout_days:
            entry_row = entries.iloc[0] # 첫 돌파 분봉
            buy_price = entry_row['target_price']
            stop_price = buy_price * (1 - stop_loss_rate)
            sell_price = close_dict.get(date)
            is_stop_loss = False

            # 당일 최저가 기반 손절 여부 판단
            day_min_full = min_df[min_df['date'] == date]
            if day_min_full['low'].min() <= stop_price:
                sell_price = stop_price
                is_stop_loss = True

            if sell_price:
                # 정밀 수익률 계산 (비용 차감)
                profit_rate = (sell_price / buy_price) - 1 - total_cost_ratio
                
                # [데이터 결합 시작]
                # 1. 기본 매매 정보
                trade_data = {
                    'ticker': ticker, 
                    'date': date, 
                    'profit': profit_rate,
                    'buy_price': buy_price, 
                    'sell_price': sell_price,
                    'is_stop_loss': is_stop_loss, 
                    'entry_time': entry_row['time']
                }
                
                # 2. 분봉 지표 추가 (m_ 접두어 부여하여 모든 컬럼 강제 병합)
                min_row_dict = entry_row.to_dict()
                for k, v in min_row_dict.items():
                    if k not in ['ticker', 'date', 'time', 'target_price']:
                        trade_data[f"m_{k}"] = v
                
                # 3. 일봉 지표 추가 (d_ 접두어 부여)
                current_day_indicators = day_dict.get(date, {})
                for k, v in current_day_indicators.items():
                    # 중복 방지를 위해 target_price, close 등은 제외
                    if k not in ['target_price', 'close', 'open', 'high', 'low', 'volume']:
                        trade_data[f"d_{k}"] = v
                
                logs.append(trade_data)
                
        return logs

    def _add_to_summary(self, ticker, trade_logs):
        df = pd.DataFrame(trade_logs)
        profits = df['profit']
        
        # 요약 통계 계산 및 리스트 추가
        self.total_summary_logs.append({
            'ticker': ticker,
            'total_return': ((1 + profits).prod() - 1) * 100,
            'win_rate': (profits > 0).mean() * 100,
            'trade_count': len(df),
            'avg_profit': profits.mean() * 100
        })
        
# 삼성전자로 테스트
# ... (기존 클래스 코드 하단에 추가) ...

if __name__ == "__main__":
    from Strategy.volatility_breakout import VolatilityBreakout
    
    # 1. 테스트용 종목 설정 (삼성전자)
    ticker = "005930" # [cite: 1, 28, 213]
    
    # 2. 데이터 경로 설정 (프로젝트 루트 기준)
    # BASE_DIR 설정이 상단에 완료되어 있어야 합니다.
    minute_path = os.path.join(BASE_DIR, "data", "chart", "minute", f"{ticker}.parquet") #  [cite: 6, 187]
    daily_path = os.path.join(BASE_DIR, "data", "chart", "daily", f"{ticker}.parquet") # [cite: 6, 28]
    
    print(f"[*] [{ticker}] 테스트 및 저장 프로세스를 시작합니다.")
    
    try:
        # 3. 데이터 파일 존재 확인
        if not os.path.exists(minute_path) or not os.path.exists(daily_path):
            print(f"[!] 에러: 데이터 파일이 없습니다. 경로를 확인하세요.") # [cite: 6]
        else:
            # 4. 데이터 로드
            m_df = pd.read_parquet(minute_path) # [cite: 6, 187]
            d_df = pd.read_parquet(daily_path) # [cite: 6, 28]
            
            # 5. 전략 및 백테스터 초기화 (k=0.5)
            test_strategy = VolatilityBreakout(k=0.5) # [cite: 18]
            backtester = VolatilityBacktest(test_strategy) # [cite: 4]
            
            # 6. 실행 (save=True를 추가하여 Parquet 파일 저장을 활성화)
            print(f"[*] 백테스트 엔진 가동 및 상세 내역 저장 중...")
            success = backtester.run(ticker, m_df, d_df, save=True) # [cite: 5, 12]
            
            if success and backtester.total_summary_logs:
                res = backtester.total_summary_logs[0] # [cite: 1]
                print("\n" + "="*45)
                print(f"   [{ticker}] 백테스트 결과 요약")
                print("-" * 45)
                print(f"   누적 수익률   : {res['total_return']:>10.2f}%")
                print(f"   승률          : {res['win_rate']:>10.2f}%")
                print(f"   총 거래 횟수  : {res['trade_count']:>10}회")
                print("="*45)
                
                # 저장된 파일 위치 안내
                save_file = os.path.join(backtester.output_dir, f"{ticker}.parquet") # [cite: 6]
                if os.path.exists(save_file):
                    print(f"[*] 상세 매매 내역이 저장되었습니다: \n    {save_file}") # [cite: 6]
            else:
                print("[!] 백테스트 결과가 없습니다.")
                
    except Exception as e:
        print(f"[!] 테스트 실행 중 오류 발생: {e}")
        import traceback
        traceback.print_exc()