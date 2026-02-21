import pandas as pd
import numpy as np
import os
import yfinance as yf
import FinanceDataReader as fdr  # 권장: 한국 주식 데이터 전용 라이브러리
from tqdm import tqdm
import warnings

warnings.filterwarnings('ignore')

class AdvancedContextSimulator:
    def __init__(self):
        self.report_path = os.path.join('data', 'backtest', 'volatility', 'summary', 'total_backtest_report.csv')
        self.result_dir = os.path.join('data', 'backtest', 'volatility', 'result')
        self.code_path = 'stock_codes.csv' # 업로드된 파일 경로 확인 필요
        
        # 1. 지수 데이터 확보 (FDR 우선 사용 -> YFinance 백업)
        print("📥 시장 지수 데이터 다운로드 중...")
        try:
            self.kospi = fdr.DataReader('KS11', '2024-01-01', '2026-03-01')
            self.kosdaq = fdr.DataReader('KQ11', '2024-01-01', '2026-03-01')
            print(f"✅ FDR 데이터 확보 완료 (KOSPI: {len(self.kospi)}건)")
        except:
            print("⚠️ FDR 실패, YFinance 시도...")
            self.kospi = yf.download('^KS11', start='2024-01-01', end='2026-03-01', progress=False)
            self.kosdaq = yf.download('^KQ11', start='2024-01-01', end='2026-03-01', progress=False)
        
        # 데이터 검증
        if self.kospi.empty or self.kosdaq.empty:
            print("❌ 지수 데이터 다운로드 실패. 인터넷 연결을 확인하세요.")
            self.market_data_ok = False
        else:
            self.market_data_ok = True
            # 이동평균선 계산
            for df in [self.kospi, self.kosdaq]:
                df['MA5'] = df['Close'].rolling(5).mean()
                df['MA20'] = df['Close'].rolling(20).mean()

        # 2. 섹터 정보 로드
        if os.path.exists(self.code_path):
            self.codes = pd.read_csv(self.code_path)
            self.codes['code'] = self.codes['code'].astype(str).str.zfill(6)
        else:
            self.codes = pd.DataFrame()

    def get_market_status(self, date, market_type='KOSPI'):
        if not self.market_data_ok: return True # 데이터 없으면 통과(기존 오류 원인)
        
        market_df = self.kospi if 'KOSPI' in str(market_type).upper() else self.kosdaq
        target_date = pd.to_datetime(date).tz_localize(None)
        
        try:
            # 해당 날짜의 데이터 조회 (asof)
            idx = market_df.index.get_indexer([target_date], method='pad')[0]
            if idx == -1: return True
            row = market_df.iloc[idx]
            
            # 필터: 지수가 5일선 위에 있는가? (상승 추세)
            return row['Close'] > row['MA5']
        except:
            return True

    def run_simulation(self):
        if not os.path.exists(self.report_path):
            print("❌ 리포트 파일이 없습니다.")
            return

        report = pd.read_csv(self.report_path)
        report['Ticker'] = report['Ticker'].astype(str).str.zfill(6)
        
        # 섹터 정보 병합
        if not self.codes.empty:
            report = pd.merge(report, self.codes[['code', 'upName', 'market_type']], 
                            left_on='Ticker', right_on='code', how='left')
        
        results = {'Original': [], 'Market_Filter': [], 'Sector_Filter': [], 'Combined': []}
        
        # 강세 섹터 선정 (상위 3개)
        if 'upName' in report.columns:
            top_sectors = report.groupby('upName')['Total Return [%]'].mean().nlargest(3).index.tolist()
            print(f"🌟 현재 강세 섹터 Top 3: {top_sectors}")
        else:
            top_sectors = []

        print("🚀 통합 시뮬레이션 시작 (샘플링)...")
        # target_tickers = report['Ticker'].unique()[:200] # 속도를 위해 200개만
        target_tickers = report['Ticker'].unique() 

        for ticker in tqdm(target_tickers):
            trade_path = os.path.join(self.result_dir, f"trades_{ticker}.parquet")
            if not os.path.exists(trade_path): continue
            
            trades = pd.read_parquet(trade_path)
            ticker_info = report[report['Ticker'] == ticker].iloc[0]
            market_type = ticker_info.get('market_type', 'KOSDAQ')
            sector = ticker_info.get('upName', 'Unknown')
            
            for _, trade in trades.iterrows():
                entry_date = pd.to_datetime(trade['entry_date']).tz_localize(None)
                pnl = trade.get('return', 0)
                
                # 1. 원본
                results['Original'].append(pnl)
                
                # 2. 마켓 타이밍 (지수 필터)
                is_bull_market = self.get_market_status(entry_date, market_type)
                if is_bull_market:
                    results['Market_Filter'].append(pnl)
                
                # 3. 섹터 필터 (강세 섹터만)
                is_good_sector = sector in top_sectors
                if is_good_sector:
                    results['Sector_Filter'].append(pnl)
                
                # 4. 통합 필터 (둘 다 만족)
                if is_bull_market and is_good_sector:
                    results['Combined'].append(pnl)

        # 결과 출력
        print("\n" + "="*60)
        print(f"{'필터 종류':<15} | {'거래 횟수':<10} | {'평균 수익률':<12} | {'승률':<10}")
        print("-" * 60)
        for key, vals in results.items():
            if not vals: continue
            avg_ret = np.mean(vals) * 100
            win_rate = (np.array(vals) > 0).mean() * 100
            count = len(vals)
            print(f"{key:<15} | {count:<10} | {avg_ret:>10.2f}% | {win_rate:>8.2f}%")
        print("="*60)

if __name__ == "__main__":
    sim = AdvancedContextSimulator()
    sim.run_simulation()