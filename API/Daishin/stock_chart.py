import win32com.client
import pandas as pd
# Import the DaishinAPI base class
from api import DaishinAPI


class CpStockChart(DaishinAPI):
    def __init__(self):
        super().__init__() 
        # 1. 연결확인 객체 
        # self.obj_CpCybos = win32com.client.Dispatch("CpUtil.CpCybos")
        # 2. 호출 제한 객체
        # self.obj_CpLimit = win32com.client.Dispatch("CpUtil.CpLimit")
        
        # 부모에서 속성이 초기화되지 않았을 수 있으므로 안전하게 가져옴
        self.obj_CpCybos = getattr(self, "obj_CpCybos", None)
        self.obj_CpLimit = getattr(self, "obj_CpLimit", None)
        
        if self.obj_CpLimit is not None:
            self.obj_stock_chart = win32com.client.Dispatch("CpSysDib.StockChart")
        else:
            self.obj_stock_chart = None

    def Request(self, 
                code,
                retrieve_type="1",           
                fromDate = None,
                toDate=None,
                retrieve_limit= 500,
                caller=None,
                chart_type='D',
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
        self.wait_for_limit(limit_type=1)  # 시세 요청 관련
        
        self.obj_stock_chart.SetInputValue(0, code)  # 종목코드
        self.obj_stock_chart.SetInputValue(1, ord(retrieve_type)) # 기간으로 받기 또는 개수로 받기
        if toDate is not None:
            self.obj_stock_chart.SetInputValue(2, toDate)  # To 날짜
        if fromDate is not None:
            self.obj_stock_chart.SetInputValue(3, fromDate)  # From 날짜
            
        self.obj_stock_chart.SetInputValue(4, retrieve_limit)  # 최근 500일치
        
        self.obj_stock_chart.SetInputValue(5, [0, 2, 3, 4, 5, 8] if retrieve_type == "1" else [0, 1, 2, 3, 4, 5])  # 날짜,시가,고가,저가,종가,거래량
        
        self.obj_stock_chart.SetInputValue(6, ord(chart_type))  # '차트 주기
        
        if chart_type in ['m', 'T']:
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
        times = [] if retrieve_type == "2" else None
        opens = []
        highs = []
        lows = []
        closes = []
        vols = []

        for i in range(record_count):
            dates.append(self.obj_stock_chart.GetDataValue(0, i))
            if retrieve_type == "2":
                times.append(self.obj_stock_chart.GetDataValue(1, i))
            opens.append(self.obj_stock_chart.GetDataValue(2, i))
            highs.append(self.obj_stock_chart.GetDataValue(3, i))
            lows.append(self.obj_stock_chart.GetDataValue(4, i))
            closes.append(self.obj_stock_chart.GetDataValue(5, i))
            vols.append(self.obj_stock_chart.GetDataValue(6, i))

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
            'open': opens,
            'high': highs,
            'low': lows,
            'close': closes,
            'volume': vols,
        }
        if times is not None:
            data['time'] = times

        df = pd.DataFrame(data)

        print(f"조회 완료: {record_count}개 레코드")
        return df