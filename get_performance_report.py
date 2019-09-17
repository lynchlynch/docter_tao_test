import tushare as ts
import time
import math
import pandas as pd
import zeroize
import numpy as np

start_time = time.time()

#下载之前完整年份的财报
years = [2008,2009,2010,2011,2012,2013,2014,2015,2016,2017,2018]
quaters = [1,2,3,4]
# main_report_rootpath = '/Users/pei/PycharmProjects/Raw Data/Report/PerReport/MainReport/'
main_report_rootpath = 'D:/pydir/Raw Data/Report/PerReport/MainReport/'
# profit_rootpath = '/Users/pei/PycharmProjects/Raw Data/Report/PerReport/Profit/'
profit_rootpath = 'D:/pydir/Raw Data/Report/PerReport/Profit/'
for year in years:#[:1]:

    for quater in quaters:#[:2]:
        ##下载财务主表
        stock_report_tmp = ts.get_report_data(year,quater)
        stock_report_tmp.drop_duplicates(subset=['code'], keep='first', inplace=True)#删除重复股票
        stock_report_tmp.fillna(value = 0xffff, inplace = True)#将空值替换为65535
        savepath = main_report_rootpath + str(year) + '_' + str(quater) + '.csv'
        stock_report_tmp.to_csv(savepath,encoding='utf_8_sig')
        ##下载盈利表
        stock_profit_tmp = ts.get_profit_data(year,quater)
        savepath_profit = profit_rootpath + str(year) + '_' + str(quater) + '.csv'
        stock_profit_tmp.drop_duplicates(subset=['code'], keep='first', inplace=True)#删除重复股票
        stock_profit_tmp.fillna(value=0xffff, inplace=True)#将空值替换为65555
        stock_profit_tmp.to_csv(savepath_profit,encoding='utf_8_sig')

        ###将不完整的股票代码补零，如227，补为000227，以便后续比较
        for single_code in stock_report_tmp['code']:
            zeroize.zeroize(single_code)

        #同比收益有bug，未考虑到负的情况，因此做出相应修正
        '''
        if year != years[0]:
            savepath_pre = main_report_rootpath + str(year-1) + '_' + str(quater) + '.csv'#前一年的每股收益，以便计算同比增长
            stock_report_tmp_pre = pd.read_csv(savepath_pre)
            for index in range(len(stock_report_tmp_pre)):
                code_pre = str(stock_report_tmp_pre['code'][index])
                single_code_pre = zeroize.zeroize(code_pre)
                if stock_report_tmp_pre['eps'][index] < 0:
                    single_stock = stock_report_tmp.loc[stock_report_tmp['code'] == single_code_pre]
                    pos = single_stock.index
                    single_stock['eps_yoy'] *= (-1)
                    stock_report_tmp['eps_yoy'][pos] = single_stock['eps_yoy']

                #由于有些eps_yoy非常大，经分析，是因为分母为0，故从分子分母中分别剔除这类数据
                print(stock_report_tmp_pre)
                print(type(stock_report_tmp_pre[stock_report_tmp_pre['code'] == single_code_pre]['eps'].values))
                if abs(stock_report_tmp_pre[stock_report_tmp_pre['code'] == single_code_pre]['eps'].values) < 0.01:
                    stock_report_tmp_pre.drop(stock_report_tmp_pre[stock_report_tmp_pre['code']== single_code_pre].index,inplace=True)
                    stock_report_tmp.drop(stock_report_tmp[stock_report_tmp['code'] == single_code_pre].index,inplace=True)
            stock_report_tmp_pre.to_csv(savepath_pre, encoding='utf_8_sig')
            stock_report_tmp.to_csv(savepath, encoding='utf_8_sig')
        '''
        if year != years[0]:
            savepath_pre = main_report_rootpath + str(year-1) + '_' + str(quater) + '.csv'#前一年的每股收益，以便计算同比增长
            stock_report_tmp_pre = pd.read_csv(savepath_pre)
            # print(stock_report_tmp_pre['code'])
            for single_code in stock_report_tmp_pre['code']:
                # print(single_code)
                # print(stock_report_tmp_pre[stock_report_tmp_pre['code'] == single_code]['eps'])
                stock_pre_eps =  stock_report_tmp_pre[stock_report_tmp_pre['code'] == single_code]['eps'].values
                if stock_pre_eps != 0xffff :
                    if stock_pre_eps < 0:
                        stock_report_tmp[stock_report_tmp['code'] == single_code]['eps_yoy'] *= (-1)

                #由于有些eps_yoy非常大，经分析，是因为分母为0，故从分子分母中分别剔除这类数据
                    # print(stock_report_tmp_pre)
                    # print(type(stock_report_tmp_pre[stock_report_tmp_pre['code'] == single_code]['eps'].values))
                    if abs(stock_pre_eps) < 0.01:
                        stock_report_tmp_pre.drop(stock_report_tmp_pre[stock_report_tmp_pre['code']== single_code].index,inplace=True)
                        stock_report_tmp.drop(stock_report_tmp[stock_report_tmp['code'] == single_code].index,inplace=True)
            stock_report_tmp_pre.to_csv(savepath_pre, encoding='utf_8_sig')
            stock_report_tmp.to_csv(savepath, encoding='utf_8_sig')
        print("\n")

        ###计算利润表中当季收入，并新增进原表中,原表中的business_income是年内累计收入
        if quater == 1:
            income_current = np.array(stock_profit_tmp['business_income']).tolist()
            stock_profit_tmp['current_income'] = income_current
            stock_profit_tmp.to_csv(savepath_profit,encoding='utf_8_sig')

        else:
            stock_profit_pre = pd.read_csv(profit_rootpath  + str(year) + '_' + str(quater - 1) + '.csv')
            income_current = []
            stock_profit_tmp = pd.read_csv(savepath_profit)
            for single_code in stock_profit_tmp['code']:
                if stock_profit_pre[stock_profit_pre['code'] == single_code]['business_income'].empty == False:#判断股票代码在上一季度存在
                    single_income = np.array(stock_profit_tmp[stock_profit_tmp['code'] == single_code]['business_income']).tolist()[0] - \
                                    np.array(stock_profit_pre[stock_profit_pre['code'] == single_code]['business_income']).tolist()[0]
                    income_current.append(single_income)
                else:
                    income_current.append(np.NaN)

            stock_profit_tmp['current_income'] = income_current
            del stock_profit_tmp['Unnamed: 0']
            stock_profit_tmp.to_csv(savepath_profit,encoding='utf_8_sig')


#下载当年不完整的财报
current_year = 2019
current_quater = 1
stock_report_tmp = ts.get_report_data(current_year,current_quater)
stock_report_tmp.drop_duplicates(subset=['code'], keep='first', inplace=True)  # 删除重复股票
stock_report_tmp.fillna(value=0xffff, inplace=True)  # 将空值替换为65535
savepath = main_report_rootpath + str(current_year) + '_' + str(current_quater) + '.csv'
stock_profit_tmp = ts.get_profit_data(current_year,current_quater)
stock_profit_tmp.drop_duplicates(subset=['code'], keep='first', inplace=True)#删除重复股票
stock_profit_tmp.fillna(value=0xffff, inplace=True)#将空值替换为65555
savepath_profit = profit_rootpath + str(current_year) + '_' + str(current_quater) + '.csv'
stock_profit_tmp.to_csv(savepath_profit,encoding='utf_8_sig')

savepath_pre = main_report_rootpath + str(current_year-1) + '_' + str(current_quater) + '.csv'#前一年的每股收益，以便计算同比增长
stock_report_tmp_pre = pd.read_csv(savepath_pre)
for index in range(len(stock_report_tmp_pre)):
    code_pre = str(stock_report_tmp_pre['code'][index])
    single_code_pre = zeroize.zeroize(code_pre)
    zeroize.zeroize(single_code_pre)
    if stock_report_tmp_pre['eps'][index] < 0:
        single_stock = stock_report_tmp.loc[stock_report_tmp['code'] == single_code_pre]
        pos = single_stock.index
        single_stock['eps_yoy'] *= (-1)
        stock_report_tmp['eps_yoy'][pos] = single_stock['eps_yoy']

stock_report_tmp.to_csv(savepath,encoding='utf_8_sig')
print("\n")

###计算利润表中当季收入，并新增进原表中,原表中的business_income是年内累计收入
if current_quater == 1:
    income_current = np.array(stock_profit_tmp['business_income']).tolist()
    stock_profit_tmp['current_income'] = income_current
    stock_profit_tmp.to_csv(savepath_profit,encoding='utf_8_sig')

else:
    stock_profit_pre = pd.read_csv(profit_rootpath + str(current_year) + '_' + str(current_quater - 1) + '.csv')
    income_current = []
    stock_profit_tmp = pd.read_csv(savepath_profit)
    for single_code in stock_profit_tmp['code']:
        if stock_profit_pre[stock_profit_pre['code'] == single_code]['business_income'].empty == False:  # 判断股票代码在上一季度存在
            single_income = \
            np.array(stock_profit_tmp[stock_profit_tmp['code'] == single_code]['business_income']).tolist()[0] - \
            np.array(stock_profit_pre[stock_profit_pre['code'] == single_code]['business_income']).tolist()[0]
            income_current.append(single_income)
        else:
            income_current.append(np.NaN)

    stock_profit_tmp['current_income'] = income_current
    del stock_profit_tmp['Unnamed: 0']
    stock_profit_tmp.to_csv(savepath_profit,encoding='utf_8_sig')

end_time = time.time()
print(end_time - start_time)


