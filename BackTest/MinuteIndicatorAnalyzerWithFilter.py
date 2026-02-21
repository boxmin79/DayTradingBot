import pandas as pd
import numpy as np
import os
import matplotlib.pyplot as plt
import pandas_ta as ta
from tqdm import tqdm
import warnings

warnings.filterwarnings('ignore')

class MinuteIndicatorAnalyzer:
    def __init__(self):
        # 1. 경로 설정
        self.report_path = os.path.join('data', 'backtest', 'volatility', 'summary', 'total_backtest_report.csv')
        self.result_dir = os.path.join('data', 'backtest', 'volatility', 'result')
        self.minute_dir = os.path.join('data', 'chart', 'minute')
        
        # 2. 거래 비용 설정 (0.0025 = 0.25% : 수수료+세금)
        # 백테스트 결과(pnl)에 이미 비용이 포함되어 있다면 0으로 설정하세요.
        self.TRANSACTION_COST = 0.0025 
        
        self.report = pd.read_csv(self.report_path)
        self.report['Ticker'] = self.report['Ticker'].astype(str).str.zfill(6)

    def calculate_minute_indicators(self, df):
        """
        진입 직전 분봉 데이터 지표 계산
        ★ 중요: 모든 지표는 계산 후 shift(1)을 하여 미래 참조를 원천 차단함.
        """
        # 1. 거래량 스파이크 (최근 5분 평균 / 60분 평균)
        v_ma5 = df['volume'].rolling(5).mean()
        v_ma60 = df['volume'].rolling(60).mean()
        df['vol_spike'] = v_ma5 / (v_ma60 + 1e-9)

        # 2. 가격 가속도 (Momentum) - 10분 전 대비 수익률
        df['mom_10'] = (df['close'] / df['close'].shift(10) - 1) * 100

        # 3. 당일 고가 대비 위치 (60분 내 최고가 기준)
        df['dist_high'] = (df['close'] / df['high'].rolling(60).max() - 1) * 100

        # 4. RSI (분봉상 과매수 여부)
        df['rsi_min'] = ta.rsi(df['close'], length=14)

        # ★ [핵심 수정] 미래 참조 방지 (Look-ahead Bias Prevention)
        # 09:05분에 진입하려면 09:04분까지 완성된 지표를 봐야 함.
        # 따라서 모든 지표를 한 칸씩 뒤로 미룸.
        cols_to_shift = ['vol_spike', 'mom_10', 'dist_high', 'rsi_min']
        df[cols_to_shift] = df[cols_to_shift].shift(1)

        return df

    def apply_filters(self, df):
        """데이터 기반 도출된 필터 조건 적용"""
        filtered_df = df[
            (df['vol_spike'] > 1.5) & 
            (df['rsi_min'] > 60) & 
            (df['mom_10'] > 0.5)
        ].copy()
        return filtered_df

    def get_metrics(self, df):
        """수익성 및 리스크 지표 계산 (단리 기준 수정)"""
        if df.empty: return None
        
        # 1. 누적 수익 곡선 (단리 합산: Cumulative Sum)
        # 복리(cumprod)는 손익비가 1 미만일 때 0으로 수렴하므로, 
        # 전략의 순수 Edge를 보기 위해 단리로 계산.
        equity_curve = df['net_return'].cumsum() * 100 # % 단위로 변환해서 누적
        
        wins = df[df['net_return'] > 0]['net_return']
        losses = df[df['net_return'] <= 0]['net_return']
        
        # 승률 및 손익비
        win_rate = (len(wins) / len(df)) * 100
        pf = wins.sum() / abs(losses.sum()) if losses.sum() != 0 else 0
        
        # MDD (단리 기준: 고점 대비 하락 폭)
        running_max = equity_curve.cummax()
        drawdown = running_max - equity_curve
        mdd = drawdown.max()
        
        return {
            'Equity': equity_curve,
            'Total Return': equity_curve.iloc[-1], # 단순 합산 수익률 (%)
            'Win Rate': win_rate,
            'Profit Factor': pf,
            'Avg Win': wins.mean() * 100 if not wins.empty else 0,
            'Avg Loss': losses.mean() * 100 if not losses.empty else 0,
            'MDD': mdd, # % 포인트 단위
            'Trade Count': len(df)
        }

    def get_minute_analysis(self, ticker):
        t_str = str(ticker).zfill(6)
        trade_path = os.path.join(self.result_dir, f"trades_{t_str}.parquet")
        minute_path = os.path.join(self.minute_dir, f"{t_str}.parquet")
        
        if not (os.path.exists(trade_path) and os.path.exists(minute_path)):
            return None
            
        try:
            trades_df = pd.read_parquet(trade_path)
            m_df = pd.read_parquet(minute_path)
            
            # 시간 인덱스 처리
            if 'date' in m_df.columns:
                m_df['date'] = pd.to_datetime(m_df['date']).dt.tz_localize(None)
                m_df.set_index('date', inplace=True)
            elif not isinstance(m_df.index, pd.DatetimeIndex):
                m_df.index = pd.to_datetime(m_df.index).dt.tz_localize(None)

            # 지표 계산 (내부에서 shift(1) 적용됨)
            m_df = self.calculate_minute_indicators(m_df)
            
            analysis_results = []
            for _, trade in trades_df.iterrows():
                entry_dt = pd.to_datetime(trade['entry_date']).tz_localize(None)
                
                # 매수 시점의 데이터 추출 (shift되었으므로 해당 시간의 행을 가져오면 됨)
                # 단, 안전을 위해 entry_dt보다 같거나 작은 마지막 데이터를 사용
                try:
                    # entry_dt 시점의 데이터가 있으면 가져오고, 없으면 직전 데이터 (asof)
                    ind_idx = m_df.index.get_indexer([entry_dt], method='pad')[0]
                    if ind_idx == -1: continue
                    ind = m_df.iloc[ind_idx]
                except:
                    continue
                
                # [수정] 수익률 단위 정규화
                raw_return = trade.get('return', trade.get('pnl', 0))
                
                # 1.0 (100%) 보다 크면 퍼센트로 간주하고 100으로 나눔, 아니면 소수로 간주
                # (데이터 특성에 따라 이 로직은 조정 필요. 여기서는 0.5(50%)를 기준으로 잡음)
                real_return = raw_return / 100.0 if abs(raw_return) > 0.5 else raw_return
                
                # 수수료 차감 (이미 차감된 데이터라면 self.TRANSACTION_COST를 0으로 설정)
                net_return = real_return - self.TRANSACTION_COST

                analysis_results.append({
                    'is_win': net_return > 0,
                    'net_return': net_return, # 수수료 차감 후 수익률
                    'vol_spike': float(ind.get('vol_spike', np.nan)),
                    'mom_10': float(ind.get('mom_10', np.nan)),
                    'dist_high': float(ind.get('dist_high', np.nan)),
                    'rsi_min': float(ind.get('rsi_min', np.nan))
                })
            return analysis_results
        except Exception:
            return None

    def run_analysis(self):
        valid_tickers = self.report[self.report['Total Trades'] > 0]['Ticker'].unique()
        all_trade_data = []
        
        print(f"🚀 [논리 수정본] 분봉 기반 수익률 심층 분석 시작 ({len(valid_tickers)}개 종목)...")
        for ticker in tqdm(valid_tickers):
            res = self.get_minute_analysis(ticker)
            if res: all_trade_data.extend(res)
                
        if not all_trade_data:
            print("데이터를 찾을 수 없습니다.")
            return

        df_res = pd.DataFrame(all_trade_data).dropna()
        
        # 필터 적용
        df_filtered = self.apply_filters(df_res)

        # 지표 계산
        m_base = self.get_metrics(df_res)
        m_filt = self.get_metrics(df_filtered)

        # 1. 성과 비교 출력
        print("\n" + "="*70)
        print(f"{'지표 (Metric)':<20} | {'필터 전 (Base)':<20} | {'필터 후 (Filtered)':<20}")
        print("-" * 70)
        
        # 출력할 키 순서 정의
        keys = ['Trade Count', 'Win Rate', 'Total Return', 'Profit Factor', 'Avg Win', 'Avg Loss', 'MDD']
        
        for key in keys:
            # 단위 설정 (% 표시)
            is_percent = key in ['Win Rate', 'Total Return', 'Avg Win', 'Avg Loss', 'MDD']
            unit = "%" if is_percent else ""
            
            val_base = m_base[key]
            val_filt = m_filt[key]
            
            print(f"{key:<20} | {val_base:>18.2f}{unit} | {val_filt:>18.2f}{unit}")
            
        print("="*70)
        print(f"※ Total Return 및 MDD는 '단리(Simple Sum)' 기준입니다. (-100% 파산 오류 방지용)")
        print(f"※ 적용된 수수료(Cost): {self.TRANSACTION_COST * 100:.2f}%")

        # 2. 수익 곡선 시각화
        plt.figure(figsize=(12, 6))
        
        # 0부터 시작하도록 조정 (Equity Curve 시각화용)
        base_curve = np.insert(m_base['Equity'].values, 0, 0)
        filt_curve = np.insert(m_filt['Equity'].values, 0, 0)
        
        plt.plot(base_curve, label=f"Base (TR: {m_base['Total Return']:.1f}%)", color='gray', alpha=0.5)
        plt.plot(filt_curve, label=f"Filtered (TR: {m_filt['Total Return']:.1f}%)", color='blue', linewidth=2)
        
        plt.title("Cumulative Net Return (Simple Sum)", fontsize=14, fontweight='bold')
        plt.xlabel("Trade Count")
        plt.ylabel("Cumulative Return (%)")
        plt.axhline(0, color='black', linestyle='--', alpha=0.3) # 0점 기준선
        plt.legend()
        plt.grid(True, linestyle='--', alpha=0.6)
        plt.tight_layout()
        
        plt.savefig('equity_curve_corrected.png')
        print(f"\n✅ 분석 완료! 수정된 수익 곡선이 'equity_curve_corrected.png'로 저장되었습니다.")

if __name__ == "__main__":
    analyzer = MinuteIndicatorAnalyzer()
    analyzer.run_analysis()