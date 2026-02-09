import sys
import os
import win32com.client
import pandas as pd

# Add project root to sys.path to resolve API package
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

# Import the DaishinAPI base class
from API.Daishin.api import DaishinAPI


class CpStockChart(DaishinAPI):
    def __init__(self):
        # 부모 클래스 초기화 (limit_type=1: 시세/차트 조회 제한 적용)
        super().__init__(limit_type=1)
        
        if self.obj_CpCybos is not None:
            self.obj_stock_chart = win32com.client.Dispatch("CpSysDib.StockChart")
        else:
            self.obj_stock_chart = None

    def request(self, 
                code,
                retrieve_type="1",
                toDate=None,
                fromDate=None,
                retrieve_limit=500,           
                caller=None,
                chart_type='D',
                # 'D'	일봉 (Day)	가장 많이 쓰임 (시간 데이터 불필요)
                # 'W'	주봉 (Week)	중장기 분석용 (시간 데이터 불필요)
                # 'M'	월봉 (Month)	장기 추세용 (시간 데이터 불필요)
                # 'm'	분봉 (minute)	단타/주간 매매 필수 (시간 데이터 필수)
                # 'S'	초봉 (Second)	초단타(스캘핑)용 (시간 데이터 필수)
                interval=1,
                gap_adjustment="2",
                adjust_price="1",
                ):
        
        # 연결 여부 체크
        if self.obj_CpCybos is None:
            print("!!! [오류] 대신증권 API 연결 객체가 없습니다.")
            return False
        
        bConnect = self.obj_CpCybos.IsConnect
        if (bConnect == 0):
            print("PLUS가 정상적으로 연결되지 않음. ")
            return False
 
        if self.obj_stock_chart is None:
            print("!!! [오류] 대신증권 API 주식차트 객체가 없습니다.")
            return False
        
        # 호출 제한 대기
        self.wait_for_limit()  # 이미 __init__에서 limit_type=1로 설정했으므로 인자 없이 호출
        
        self.obj_stock_chart.SetInputValue(0, code)  # 종목코드
        self.obj_stock_chart.SetInputValue(1, ord(retrieve_type)) # "1"기간으로  "2"개수로 받기
        if retrieve_type == "1":
            if toDate is not None:
                self.obj_stock_chart.SetInputValue(2, toDate)  # To 날짜
            if fromDate is not None:
                self.obj_stock_chart.SetInputValue(3, fromDate)  # From 날짜
        if retrieve_type == "2":
            self.obj_stock_chart.SetInputValue(4, retrieve_limit)  # 최근 500일치
        
        if chart_type in ['m', 's', 'T']:
            self.obj_stock_chart.SetInputValue(5, [0, 1, 2, 3, 4, 5, 8]) ## 날짜,시간, 시가,고가,저가,종가,거래량
        else:
            self.obj_stock_chart.SetInputValue(5, [0, 2, 3, 4, 5, 8])  # 날짜,시가,고가,저가,종가,거래량
        
        self.obj_stock_chart.SetInputValue(6, ord(chart_type))  # '차트 주기
        
        if chart_type in ['m', 's', 'T']:
            self.obj_stock_chart.SetInputValue(7, interval)  # 분간 차트 요청 시 분 간격
        
        self.obj_stock_chart.SetInputValue(8, ord(gap_adjustment))  # 갭보정
        
        self.obj_stock_chart.SetInputValue(9, ord(adjust_price))  # 수정주가 사용
        
        # 동기식 호출
        try:
            self.obj_stock_chart.BlockRequest()
        except Exception as e:
            print("BlockRequest 실패:", e)
            return False
 
        rqStatus = self.obj_stock_chart.GetDibStatus()
        rqRet = self.obj_stock_chart.GetDibMsg1()
        print("통신상태", rqStatus, rqRet)
        if rqStatus != 0:
            print(f"API 오류: {rqRet}")
            return False
 
        try:
            record_count = int(self.obj_stock_chart.GetHeaderValue(3))
        except Exception:
            print("헤더값 조회 실패")
            return False
        
        if record_count <= 0:
            print("조회 데이터가 없습니다.")
            return False
 
        # collect into local lists (caller may be None)
        dates = []
        # Determine if time data is expected based on chart_type
        expect_time_data = chart_type in ['m', 's', 'T']
        times = [] if expect_time_data else None
        opens = []
        highs = []
        lows = []
        closes = []
        vols = []
        
        for i in range(record_count):
            dates.append(self.obj_stock_chart.GetDataValue(0, i))
            
            # Current index for retrieving data from the requested fields list
            current_field_index_in_response = 1 

            if expect_time_data:
                times.append(self.obj_stock_chart.GetDataValue(current_field_index_in_response, i))
                current_field_index_in_response += 1

            opens.append(self.obj_stock_chart.GetDataValue(current_field_index_in_response, i))
            current_field_index_in_response += 1
            highs.append(self.obj_stock_chart.GetDataValue(current_field_index_in_response, i))
            current_field_index_in_response += 1
            lows.append(self.obj_stock_chart.GetDataValue(current_field_index_in_response, i))
            current_field_index_in_response += 1
            closes.append(self.obj_stock_chart.GetDataValue(current_field_index_in_response, i))
            current_field_index_in_response += 1
            vols.append(self.obj_stock_chart.GetDataValue(current_field_index_in_response, i))
        
            
        # set attributes on caller for backward compatibility
        if caller is not None:
            try:
                caller.dates = dates
                caller.times = times if times is not None else None
                caller.opens = opens
                caller.highs = highs
                caller.lows = lows
                caller.closes = closes
                caller.vols = vols
            except Exception:
                pass

        # build pandas DataFrame and return it
        data = {
            'date': dates,
        }
        if times is not None:
            data['time'] = times

        data['open'] = opens
        data['high'] = highs
        data['low'] = lows
        data['close'] = closes
        data['volume'] = vols

        df = pd.DataFrame(data)

        print(f"조회 완료: {record_count}개 레코드")
        return df
    
# 여러가지 테스트 코드
if __name__ == "__main__":
    stock_chart = CpStockChart()
    # 각 인덱스 조회 테스트
    
    df = stock_chart.request(code="A005930", retrieve_type="2", retrieve_limit=10, chart_type='D')
    if df is not False:
        print(df)
        df = stock_chart.request(code="A005930", retrieve_type="2", retrieve_limit=10, chart_type='m', interval=5)
        if df is not False:
            print(df)   
            
    # 분봉 개수 조회 테스트
    df = stock_chart.request(code="A005930", retrieve_type="2", retrieve_limit=100)
    if df is not False:
        print(df)
    # 일봉 기간 조회 테스트
    df = stock_chart.request(code="A005930", retrieve_type="1", fromDate=20230101, toDate=20231231)
    if df is not False:
        print(df)
                                     
                             
    