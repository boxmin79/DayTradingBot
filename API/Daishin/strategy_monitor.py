import sys
import os
import pandas as pd
import win32com.client
from PyQt5.QtWidgets import QApplication
from datetime import datetime

# 프로젝트 루트 경로 추가
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(BASE_DIR)

from API.Kiwoom.api import KiwoomAPI
from Strategy.volatility_breakout import VolatilityBreakout
from Indicators.factory import IndicatorFactory

# 대신증권 실시간 수신 클래스
class DaishinRealtimeReceiver:
    def __init__(self, ticker, target_price, strategy_params, parent):
        self.ticker = ticker
        self.target_price = target_price
        self.params = strategy_params # 일봉 지표 필터 데이터
        self.parent = parent
        self.is_bought = False

    def OnReceived(self):
        # 실시간 현재가 수신
        cur_price = self.parent.obj_realtime.GetHeaderValue(13) # 현재가
        
        # 1. 가격 돌파 체크
        if not self.is_bought and cur_price >= self.target_price:
            # 2. 전략 필터 조건 체크 (VolatilityBreakout 로직 활용)
            if self.check_strategy_filters():
                self.parent.execute_order(self.ticker, cur_price)
                self.is_bought = True

    def check_strategy_filters(self):
        """기존 strategy_filter에서 도출된 필터 수치 검증"""
        # MACD 기울기, 몸통비율, 밴드위치 등 미리 계산된 params 활용
        if self.params['macd_hist_slope'] > 30 and \
           self.params['body_ratio'] > 0.5 and \
           self.params['band_p'] > 0.6:
            return True
        return False

class StrategyMonitor:
    def __init__(self):
        self.strategy = VolatilityBreakout(k=0.5)
        self.kiwoom = KiwoomAPI()
        self.monitoring_csv = os.path.join(BASE_DIR, "data", "ticker", "monitoring_tickers.csv")
        
        # 실시간 수신 객체 관리
        self.obj_realtime = win32com.client.Dispatch("DsBiLib.StockCur")
        self.receivers = {}

    def init_trader(self):
        """로그인 및 초기 데이터 세팅"""
        print("[*] 시스템 초기화 중...")
        self.kiwoom.comm_connect() # 키움 주문용 로그인
        
        if not os.path.exists(self.monitoring_csv):
            print(f"[!] 감시 리스트 파일이 없습니다: {self.monitoring_csv}")
            return False

        df = pd.read_csv(self.monitoring_csv)
        for _, row in df.iterrows():
            ticker = str(row['Ticker']).zfill(6)
            # 실시간 감시에 필요한 파라미터 묶음 (일봉 기준)
            params = {
                'macd_hist_slope': row['Sharpe Ratio'], # 실제 지표값으로 확장 가능
                'body_ratio': 0.6, # 예시 기준값
                'band_p': 0.7      # 예시 기준값
            }
            # 전일 데이터를 기반으로 목표가 계산 로직 추가 필요
            target_price = 0 # 실제 계산값 대입
            
            # 실시간 수신 등록
            handler = DaishinRealtimeReceiver(ticker, target_price, params, self)
            self.receivers[ticker] = handler
            
        print(f"[✔] {len(self.receivers)}개 종목 실시간 감시 준비 완료.")
        return True

    def execute_order(self, ticker, price):
        """키움증권을 통한 실제 매수 주문 전송"""
        print(f"[🔥 SIGNAL] {ticker} 목표가 돌파! 매수 주문 전송 (가격: {price})")
        # self.kiwoom.send_order(...) 호출 로직 구현 예정
        pass

if __name__ == "__main__":
    app = QApplication(sys.argv)
    monitor = StrategyMonitor()
    if monitor.init_trader():
        sys.exit(app.exec_())