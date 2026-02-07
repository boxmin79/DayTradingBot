import win32com.client
import pandas as pd
from API.Daishin.api import DaishinAPI

class DaishinStockChart(DaishinAPI): # 상속 시전
    def __init__(self):
        super().__init__() # 부모 클래스의 생성자 호출
        self.obj_stock_chart = win32com.client.Dispatch("CpSysDib.StockChart")

    def get_stock_chart(self, code, count, chart_type, interval):
        # 수집 전 제한 확인 (부모의 메서드 사용)
        self.wait_for_limit(1)

        self.obj_stock_chart.SetInputValue(0, code)
        self.obj_stock_chart.SetInputValue(1, ord('2')) # 개수 기준
        self.obj_stock_chart.SetInputValue(4, count)
        self.obj_stock_chart.SetInputValue(5, [0, 1, 2, 3, 4, 5, 8])
        self.obj_stock_chart.SetInputValue(6, ord(chart_type))
        self.obj_stock_chart.SetInputValue(9, ord('1'))

        self.obj_stock_chart.BlockRequest()

        num_data = self.obj_stock_chart.GetHeaderValue(3)
        data_list = []
        for i in range(num_data):
            row = [self.obj_stock_chart.GetDataValue(j, i) for j in range(7)]
            data_list.append(row)

        return pd.DataFrame(data_list, columns=['date', 'time', 'open', 'high', 'low', 'close', 'volume'])