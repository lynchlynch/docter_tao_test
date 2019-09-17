#计算均线，n为指定几日均线
import numpy as np

#对数据补齐操作，主要用于对滑动平均数据的补齐。data为需要补齐的数据，n为需要补齐的数据长度,
# raw_data为原始数据，即补齐数据的来源
def zero_fill(data,raw_data,n):
    for fill_data in raw_data[:n-1]:
        data.append(fill_data)

    return data

def moving_average(stock_data,n):
    ma_result = []
    ma_result_raw = []  # 为补齐的数据
    is_empty_flag = 0
    if len(stock_data) <= n:
        is_empty_flag = 1
    else:
        ma_result = zero_fill(ma_result_raw,stock_data,n)
        for i in range(n-1,len(stock_data)):
            ma_result.append(round(sum(stock_data[i-n+1:i+1])/n, 3))

        # print(result_raw)
        # ma_result = zero_fill(ma_result_raw, stock_data, n)#补齐数据
        # print(ma_result)

    return [ma_result, is_empty_flag]