# -*- coding:utf8 -*-
import tushare as ts
import re
import numpy as np
from pandas import DataFrame
import matplotlib.pyplot as plt
import pandas as pd
import datetime
import talib as ta
import random
import pymysql.cursors



class CS(object):
    code = ''
    avg = []
    good_factor = 0.02
    bad_factor = 0.05
    cnt_bad_sell = 0
    cnt_good_buy = 0
    cnt_good_sell = 0
    cnt_risk = 0
    af = []
    fq = []
    process = []
    dd_list = []
    dd_list_show = []
    macd_list = []
    kdj_list = []
    bool_up = 0.00
    bool_mid = 0.00
    bool_dn = 0.00
    data_train = []
    data_target = []
    close = []
    test_case = []

    def __init__(self, in_code):
        self.collectDATA(in_code)

    def collectDATA(self,in_code):
        self.code = in_code

        self.data_src = temp_day = ts.get_k_data(code=in_code)
        if temp_day.shape[0] < 150 :
            raise Exception
        list_open = np.array(temp_day.iloc[temp_day.shape[0]-20:temp_day.shape[0], [1]].astype('float32'), dtype=np.float).ravel()
        list_close = np.array(temp_day.iloc[temp_day.shape[0]-20:temp_day.shape[0], [2]].astype('float32'), dtype=np.float).ravel()
        list_high = np.array(temp_day.iloc[temp_day.shape[0]-20:temp_day.shape[0], [3]].astype('float32'), dtype=np.float).ravel()
        list_low = np.array(temp_day.iloc[temp_day.shape[0]-20:temp_day.shape[0], [4]].astype('float32'), dtype=np.float).ravel()
        self.list_vol = np.array(temp_day.iloc[temp_day.shape[0]-20:temp_day.shape[0], [5]].astype('float32'), dtype=np.float).ravel()
        period = 20
        self.close = list_close
        self.avg = ta.MA(list_close,period)
        self.avg = [x for x in self.avg if str(x) != 'nan']
        self.good_buy = [x* (1.00 - self.good_factor) for x  in self.avg]
        self.good_sell = [x * (1.00 + self.good_factor) for x in self.avg]
        self.bad_sell = [x * (1.00 - self.bad_factor) for x in self.avg]
        self.cnt_risk = [0]*len(self.avg)
        self.cnt_good_sell = [0]*len(self.avg)
        self.cnt_good_buy = [0]*len(self.avg)
        self.cnt_bad_sell = [0]*len(self.avg)
        for a in range(len(self.avg)):
            self.cnt_bad_sell[a] = len([x for x in list_low[:a+period-1] if x <= self.bad_sell[a]])
            self.cnt_good_sell[a] = len([x for x in list_high[:a + period - 1] if x >= self.good_sell[a]])
            self.cnt_bad_sell[a] = len([x for x in list_low[:a + period - 1] if self.bad_sell[a] < x <= self.good_buy[a]])
            self.cnt_risk[a] = len([x for x in list_low[:a+period-1] if x <= list_close[a+period-1]])

        #ARFQ
        for b in range(len(self.avg)):
            af,fq,process = get_arfq(list_high[b:b+period],list_low[b:b+period],self.good_sell[b],self.bad_sell[b],self.good_buy[b])
            self.af.append(af/self.avg[-1])
            self.fq.append(fq)
            self.process.append(process)



def get_arfq(list_high,list_low,good_sell,bad_sell,good_buy):
    # 振幅af = (high-low)/len   频率 freq = 从good_selld到good_buy（或反之）的所需步长之和除以len
    af = ((list_high - list_low).sum()) / (len(list_high))
    start_flag = 0
    list_index = 0
    #list_high = [float(x) for x in list_high]
    #list_low = [float(x) for x in list_low]
    for k in range(len(list_high)):
        if list_high[len(list_high) - k - 1] >= good_sell:
            start_flag = 1
            list_index = len(list_high) - k - 1
        elif bad_sell < list_low[len(list_high) - k - 1] <= good_buy:
            start_flag = 2
            list_index = len(list_high) - k - 1
    freq_list = []
    freq = 0
    freq_step = []
    process = 0
    for l in range(list_index, len(list_high)):
        if start_flag > 0:
            if (divmod(start_flag, 2)[1] == 1) and (bad_sell < list_low[l] <= good_buy):
                freq_list.append(l)
                start_flag = start_flag + 1
            elif (divmod(start_flag, 2)[1] == 0) and (list_high[l] >= good_sell):
                freq_list.append(l)
                start_flag = start_flag + 1
        else:
            freq = 0
    if len(freq_list) == 1:
        freq = 0 - (len(list_high) - freq_list[0] - 1)
    elif len(freq_list) > 1:
        for m in range(1, len(freq_list)):
            freq_step.append(freq_list[len(freq_list) - m] - freq_list[len(freq_list) - 1 - m])
        freq = np.array(freq_step).sum() / (len(freq_step))

    if freq > 0:
        process = (len(list_high) - freq_list[-1] - 1) / (freq)
    else:
        process = 0
    return af,freq,process

if __name__ == '__main__':
    # 取股票代码清单
    src = ts.get_stock_basics()
    code_list = src.index
    code_set = set()
    for i in range(len(code_list)):
        resu = re.search(r'\d+', code_list[i]).string
        code_set.add(resu)
    print('Done:Code_Set')

    # 建立数据库连接
    db = pymysql.connect(host='127.0.0.1', user='root', passwd='admin', db='stock', charset='utf8')
    cursor = db.cursor()
    sql_done_set = "SELECT distinct stock_code FROM q1_cs"
    try:
        cursor.execute(sql_done_set)
        done_set = cursor.fetchall()
        for row in done_set:
            code_set.remove(row[0])

        cnt = 1
        total = len(code_set)
        for i in code_set:
            print(i)
            try:
                cs = CS(i)
                sql = "INSERT INTO q1_cs(stock_code,af,freq,process) VALUES ('%s', '%f', '%f','%f')" % (i, cs.af[-1], cs.fq[-1], cs.process[-1])
                cursor.execute(sql)
                db.commit()
                cnt += 1
                print('Seq: '+str(cnt)+' of ' + str(total) +'Code: ' + str(i) + '   AF: ' + str(cs.af[-1]) + '    Freq: ' + str(cs.fq[-1]) + '    Process: ' + str(cs.process[-1]))
            except Exception as excp:
                # db.rollback()
                print(str('Errr') + str(i))
        print('All Finished!')

    except Exception as excp:
        #db.rollback()
        print(str('Errr')+ str(i))

    db.close()

