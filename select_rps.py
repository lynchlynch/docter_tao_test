import pandas as pd
import os
import time
import datetime

def single_increase_rate(stock_data,rps_N,rps_index):#single_stock的长度必须大于rps_N
    # print(rps_index)
    # print(rps_N)
    ref_price = stock_data['close'][rps_index-rps_N]
    current_price = stock_data['close'][rps_index]
    increase_rate = (current_price-ref_price)/ref_price * 1000#放大1000倍
    return current_price,increase_rate
    # print(increase_rate)

def derive_date():
    now_time = datetime.datetime.now()
    year = str(now_time.year)
    if now_time.month < 10:
        month = '0' + str(now_time.month)
    else:
        month = str(now_time.month)
    if now_time.day < 10:
        day = '0' + str(now_time.day)
    else:
        day =  str(now_time.day)
    return year + '-' + month + '-' + day

def rps_sorted(stock_path,rps_N,stock_length,current_process_date):
    start_time = time.time()
    #删除不是csv的文件
    file_list = os.listdir(stock_path)
    for single_file in file_list:
        if single_file.split('.')[1] != 'csv':
            os.remove(stock_path + '/' + single_file)

    file_list = os.listdir(stock_path)
    increase_rate_list = []#[stock[code,date,rps,close]]
    initial_value = -3000#若当天无数据，则将增长率置为-3000
    for single_stock in file_list:
        # print(single_stock)
        stock_data = pd.read_csv(stock_path + '/' + single_stock)
        # print('len(single_stock):' + str(len(stock_data)))
        if len(stock_data) >= stock_length:
            if str(stock_data['date'][len(stock_data)-1]) == current_process_date:
                rps_index = stock_data[stock_data['date']==current_process_date].index.values[0]
                current_price, increase_rate = single_increase_rate(stock_data, rps_N, rps_index)
'''
                current_price,increase_rate = single_increase_rate(stock_data,rps_N,rps_index)
                single_date = stock_data['date'][rps_index]
                single_code = single_stock.split('.')[0]
                increase_rate_list.append([single_code,single_date,current_price,increase_rate])
    rps_df = pd.DataFrame(increase_rate_list,columns=['code','date','current_price','increase_rate'])
    rps_df.sort_values(by='increase_rate', axis=0, ascending=False,inplace=True)

    rps_list = []
    stock_total_num = len(rps_df)
    for df_index in range(len(rps_df)):
        single_rps = round((1-df_index/stock_total_num) *100,3)
        rps_list.append(single_rps)
    rps_df['rps'] = rps_list

    end_time = time.time()
    # print(end_time-start_time)
    # today_date = derive_date()
    # rps_df.to_csv(result_path + '/' + today_date + '_select_rps.csv')
'''