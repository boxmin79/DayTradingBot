import sys
from PyQt5.QtWidgets import QApplication
from Collector.minute_collector import MinuteCollector

def main():
    app = QApplication(sys.argv)
    collector = MinuteCollector()
    
    # 전 종목 수집 및 백테스트 실행
    collector.collect_all_tickers(run_backtest=True)
    
    print("\n[*] 모든 작업이 완료되었습니다.")

if __name__ == "__main__":
    main()