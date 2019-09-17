#由于Tushare数据放在私人服务器中，如果一次获取较多数据，容易被误认为是DDos攻击，故分组获取
import tushare as ts
import numpy as np
import time
import moving_average as ma
import pandas as pd
import os
from tqdm import tqdm

start_time = time.time()

#下载股票数据
stock_code_info = ts.get_stock_basics()#获取所有股票信息
stock_code = stock_code_info.index#提取股票代码
leap = 5#设置每组股票数
groups = list(range(0,len(stock_code)//leap + 1))#分组
no_data_stocks = []#储存无数据股票代码，以便重新下载
download_failed_stocks = []
# root_path = '/Users/pei/PycharmProjects/Raw Data/Tushare/'
# select_threshold = '2018-12-31'
root_path = 'D:/pydir/Raw Data/Tushare/'

#分组下载数据
for group in tqdm(groups,desc='group'):
    for stock_code_index in stock_code[(group-1) * leap : group * leap -1]:
        try:

            single_stock_data = ts.get_k_data(stock_code_index, start='2007-01-01')
            single_stock_savepath = root_path + stock_code_index + '.csv'
            single_stock_data.to_csv(single_stock_savepath)
            single_stock_data = pd.read_csv(single_stock_savepath)

            try:#补齐数据
                is_empty_flags = []

                [ma5, is_empty_flag] = ma.moving_average(single_stock_data['close'],5)
                is_empty_flags.append(is_empty_flag)

                [ma10,is_empty_flag] = ma.moving_average(single_stock_data['close'],10)
                is_empty_flags.append(is_empty_flag)

                [ma20, is_empty_flag] = ma.moving_average(single_stock_data['close'],20)
                is_empty_flags.append(is_empty_flag)

                [ma50, is_empty_flag] = ma.moving_average(single_stock_data['close'], 50)
                is_empty_flags.append(is_empty_flag)

                [ma60, is_empty_flag] = ma.moving_average(single_stock_data['close'], 60)
                is_empty_flags.append(is_empty_flag)

                [ma120, is_empty_flag] = ma.moving_average(single_stock_data['close'], 120)
                is_empty_flags.append(is_empty_flag)

                [ma250, is_empty_flag] = ma.moving_average(single_stock_data['close'], 250)
                is_empty_flags.append(is_empty_flag)

                if sum(is_empty_flags) > 0:
                    no_data_stocks.append(stock_code_index)
                    os.remove(single_stock_savepath)
                    continue

                single_stock_data.insert(single_stock_data.shape[1], 'ma5', ma5)
                single_stock_data.insert(single_stock_data.shape[1], 'ma10', ma10)
                single_stock_data.insert(single_stock_data.shape[1], 'ma20', ma20)
                single_stock_data.insert(single_stock_data.shape[1], 'ma50', ma50)
                single_stock_data.insert(single_stock_data.shape[1], 'ma60', ma60)
                single_stock_data.insert(single_stock_data.shape[1], 'ma120', ma120)
                single_stock_data.insert(single_stock_data.shape[1], 'ma250', ma250)


                single_stock_data.to_csv(single_stock_savepath)

            except KeyError:
                no_data_stocks.append(stock_code_index)
                os.remove(single_stock_savepath)
                continue

        except AttributeError:
            download_failed_stocks.append(stock_code_index)

#下载未进入分组的最后几只股票
for i in tqdm(stock_code[-leap:],desc='undownload stocks'):
    try:
        # print(i)
        single_stock_data = ts.get_hist_data(i)
        single_stock_savepath = 'D:/pydir/stock_anly/data/Tushare/' \
                                + i + '.csv'
        single_stock_data.to_csv(single_stock_savepath)
    except AttributeError:
        download_failed_stocks.append(i)

re_try_nums = 5#重试次数
#重新下载
re_try_list = list(range(0,re_try_nums))
for re_try_num in tqdm(re_try_list,desc='re_try'):
    for no_data_stock in download_failed_stocks:
        try:
            print(no_data_stock)
            single_stock_data = ts.get_hist_data(no_data_stock)
            single_stock_savepath = 'D:/pydir/stock_anly/data/Tushare/' \
                                    + no_data_stock + '.csv'
            single_stock_data.to_csv(single_stock_savepath)
            download_failed_stocks.remove(no_data_stock)
        except AttributeError:
            pass

print('The following stock is failed to downloaded:\n')
#储存下载失败股票代码
for download_failed_stock in tqdm(download_failed_stocks,desc='save failed downloads'):
    with open(root_path + 'downloaden_failed_stocks.txt', 'a') as file_object:
        file_object.write(download_failed_stock)
        file_object.write('\n')
#储存无数据股票
for no_data_stock in tqdm(no_data_stocks,'save the no_data_stock'):
    with open(root_path + 'no_data_stock.txt', 'a') as file_object:
        file_object.write(no_data_stock)
        file_object.write('\n')


end_time = time.time()

print(end_time - start_time)