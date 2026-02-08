import os
import pandas as pd
from ..API.Daishin.stock_chart import CpStockChart

class MinuteCollector:
    def __init__(self):
        self.api = CpStockChart()
        self.base_dir = os.getcwd()
        
        # 데이터 저장 폴더 (속도가 빠른 PKL 방식)
        self.save_dir = os.path.join(self.base_dir, "data", "chart", "minute")
        self.ticker_path = os.path.join(self.base_dir, "data", "ticker", "filtered_tickers.csv")
        os.makedirs(self.save_dir, exist_ok=True)
        
    def collect_all_tickers(self, run_backtest=False):

        if not os.path.exists(self.ticker_path):
            print(f"!!! [오류] 종목 리스트 파일이 없습니다: {self.ticker_path}")
            return

        df_tickers = pd.read_csv(self.ticker_path)
        total = len(df_tickers)
        
        for idx, row in df_tickers.iterrows():
            ticker_code = str(row['code']).zfill(6)
            ticker_name = row['name']
            
            # 진행률 표시
            progress = ((idx + 1) / total) * 100
            print(f"\n[{progress:6.2f}%] {ticker_name}({ticker_code})")
            
            try:
                # 1. 데이터 업데이트 (최대 20만행)
                df = self._update_single_ticker("A" + ticker_code, target_count=200000)
                
                # 2. 백테스트 수행
                if run_backtest and df is not None and not df.empty:
                    print(f"    => 엔진 분석 중...", end='\r')
                    _, metrics = self.backtester.run(df, ticker=ticker_code)
                    
                    if metrics:
                        metrics['종목명'] = ticker_name
                        metrics['종목코드'] = ticker_code
                        # 분석 결과 엔진에 추가
                        self.backtester.add_summary(metrics)
                        
                        # 10종목마다 통합 리포트 중간 저장
                        if len(self.backtester.total_summary_list) % 10 == 0:
                            self.backtester.save_total_report()
                            
                        print(f"    => 완료! 수익률: {metrics['총수익률']}, 매매: {metrics['매매횟수']}회")
            except Exception as e:
                print(f"    !!! {ticker_name} 처리 중 에러: {e}")

        # 모든 종목 완료 후 최종 리포트 저장
        self.backtester.save_total_report()

    def _update_single_ticker(self, code, target_count=200000):
        """개별 종목 데이터를 수집하고 PKL로 저장"""
        file_path = os.path.join(self.save_dir, f"{code[1:]}.pkl")
        
        # 기존 데이터 로드
        if os.path.exists(file_path):
            existing_df = pd.read_pickle(file_path)
        else:
            existing_df = pd.DataFrame()

        all_new = []
        is_next = False
        
        # 목표 수량 채울 때까지 반복 수집
        while (len(existing_df) + sum(len(x) for x in all_new)) < target_count:
            df = self.api.get_stock_chart(code, 2247, 'm', 1, is_next=is_next)
            
            if df is None or df.empty:
                break
                
            all_new.append(df)
            total_now = len(existing_df) + sum(len(x) for x in all_new)
            print(f"    [수집중] {total_now:,} / {target_count:,} 행 확보 중...", end='\r')
            
            # 더 이상 과거 데이터가 없으면 중단
            if not self.api.obj_stock_chart.Continue:
                break
            is_next = True

        # 새로운 데이터가 있으면 합치기
        if all_new:
            new_data = pd.concat(all_new, ignore_index=True)
            # 기존 데이터와 합친 후 중복 제거 및 정렬
            combined_df = pd.concat([existing_df, new_data], ignore_index=True)
            combined_df.drop_duplicates(subset=['date', 'time'], keep='last', inplace=True)
            combined_df.sort_values(by=['date', 'time'], inplace=True)
            
            # 목표 수량만큼만 유지 (최신 데이터 기준)
            if len(combined_df) > target_count:
                combined_df = combined_df.tail(target_count)
            
            # 파일 저장 및 반환
            combined_df.to_pickle(file_path)
            return combined_df
        
        return existing_df