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


class DC(object):
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
        obv = 150
        time_temp = datetime.datetime.now() - datetime.timedelta(obv)
        time_index = datetime.date(time_temp.year, time_temp.month, time_temp.day)

        if(time_temp.month < 10):
            month = '0'+str(time_temp.month)
        else:
            month = str(time_temp.month)
        if(time_temp.day < 10):
            day = '0'+str(time_temp.day)
        else:
            day = str(time_temp.day)
        start_dt_kday = str(time_temp.year) + '-' + str(month) + '-' + str(day)

        self.data_src = temp_day = ts.get_k_data(code=in_code, start= start_dt_kday,end='2018-01-01')
        list_open = np.array(temp_day.iloc[0:, [1]].astype('float32'), dtype=np.float).ravel()
        list_close = np.array(temp_day.iloc[0:, [2]].astype('float32'), dtype=np.float).ravel()
        list_high = np.array(temp_day.iloc[0:, [3]].astype('float32'), dtype=np.float).ravel()
        list_low = np.array(temp_day.iloc[0:, [4]].astype('float32'), dtype=np.float).ravel()
        self.list_vol = np.array(temp_day.iloc[0:, [5]].astype('float32'), dtype=np.float).ravel()
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
            self.af.append(af)
            self.fq.append(fq)
            self.process.append(process)


        # #获取大单数据
        # for m in range(obv):
        #     dd_start_dt = time_index + datetime.timedelta(m)
        #     try:
        #         temp2 = ts.get_sina_dd(self.code,dd_start_dt)
        #         error = len(temp2)
        #     except Exception as exp:
        #         temp2 = []
        #         self.dd_list.append(0)
        #         self.dd_list_show.append([0, dd_start_dt])
        #     if len(temp2) > 0:
        #         list_dd_all = temp2.iloc[0:, [4,6]]
        #         df_buy = list_dd_all[0:][list_dd_all.type =='买盘']
        #         list_buy = np.array(df_buy.iloc[0:,[0]]).ravel()
        #         df_sell = list_dd_all[0:][list_dd_all.type =='卖盘']
        #         list_sell = np.array(df_sell.iloc[0:, [0]]).ravel()
        #         dd_resu = np.array(list_buy).sum() - np.array(list_sell).sum()
        #         self.dd_list.append(dd_resu)
        #         self.dd_list_show.append([dd_resu,dd_start_dt])


        # MACD
        macd_temp = list(ta.MACD(list_close, 10, 20, 5))
        self.macd_list = [x for x in macd_temp[2] if str(x) != 'nan']


        # KDJ
        resu_kdj = kdj(temp_day)
        self.kdj_list = [x[-1] for x in resu_kdj]

        # BOLL
        bool_up_list, bool_mid_list, bool_dn_list = ta.BBANDS(list_close, timeperiod=20, nbdevup=2, nbdevdn=2, matype=0)
        self.bool_up = [x for x in bool_up_list if str(x)!='nan']
        self.bool_mid = [x for x in bool_mid_list if str(x)!='nan']
        self.bool_dn = [x for x in bool_dn_list if str(x)!='nan']

        self.get_data_src()

    def refreshDATA(self,resu):
        randseed = random.random()*random.choice([-1,1])
        new_high = max(randseed*self.af[-1],(1-randseed)*self.af[-1])
        new_low = self.af[-1]-new_high
        self.data_src.loc[self.data_src.index[-1]+1] = {'date': '1', 'open': float(resu), 'close': float(resu),'high': float(resu)+float(new_high), 'low': float(resu)-float(new_low), 'volume': self.list_vol[-1]*(1.0+randseed), 'code': self.code}
        #print(self.data_src)
        temp_day2 = self.data_src
        list_open = np.array(temp_day2.iloc[0:, [1]].astype('float32'), dtype=np.float).ravel()
        list_close = np.array(temp_day2.iloc[0:, [2]].astype('float32'), dtype=np.float).ravel()
        list_high = np.array(temp_day2.iloc[0:, [3]].astype('float32'), dtype=np.float).ravel()
        list_low = np.array(temp_day2.iloc[0:, [4]].astype('float32'), dtype=np.float).ravel()
        self.list_vol = np.array(temp_day2.iloc[0:, [5]].astype('float32'), dtype=np.float).ravel()
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
            self.af.append(af)
            self.fq.append(fq)
            self.process.append(process)

        # MACD
        macd_temp = list(ta.MACD(list_close, 10, 20, 5))
        self.macd_list = [x for x in macd_temp[2] if str(x) != 'nan']


        # KDJ
        resu_kdj = kdj(temp_day2)
        self.kdj_list = [x[-1] for x in resu_kdj]

        # BOLL
        bool_up_list, bool_mid_list, bool_dn_list = ta.BBANDS(list_close, timeperiod=20, nbdevup=2, nbdevdn=2, matype=0)
        self.bool_up = [x for x in bool_up_list if str(x)!='nan']
        self.bool_mid = [x for x in bool_mid_list if str(x)!='nan']
        self.bool_dn = [x for x in bool_dn_list if str(x)!='nan']

        self.get_data_src()



    def get_data_src(self):
        self.data_train = []
        self.data_target = []
        for i in range(1,len(self.macd_list)):
            train = [self.avg[len(self.avg)-1-i],self.list_vol[len(self.list_vol)-1-i],self.cnt_bad_sell[len(self.cnt_bad_sell)-1-i],self.cnt_good_buy[len(self.cnt_good_buy)-1-i],self.cnt_good_sell[len(self.cnt_good_sell)-1-i],self.cnt_risk[len(self.cnt_risk)-1-i],self.af[len(self.af)-1-i],self.fq[len(self.fq)-1-i],self.macd_list[len(self.macd_list)-1-i],self.kdj_list[len(self.kdj_list)-1-i],self.bool_up[len(self.bool_up)-1-i],self.bool_mid[len(self.bool_mid)-1-i],self.bool_dn[len(self.bool_dn)-1-i]]
            self.data_train.append(np.array(train))
            target = [self.close[len(self.close) - i]]
            self.data_target.append(np.array(target))
            #self.data_target.append(self.avg[len(self.avg)-i])
        self.test_case = np.array([self.avg[-1],self.list_vol[-1],self.cnt_bad_sell[-1],self.cnt_good_buy[-1],self.cnt_good_sell[-1],self.cnt_risk[-1],self.af[-1],self.fq[-1],self.macd_list[-1],self.kdj_list[-1],self.bool_up[-1],self.bool_mid[-1],self.bool_dn[-1]])
        self.data_train = np.array(self.data_train[::-1])
        self.data_target = np.array(self.data_target[::-1])



def get_arfq(list_high,list_low,good_sell,bad_sell,good_buy):
    # 振幅af = (high-low)/len   频率 freq = 从good_selld到good_buy（或反之）的所需步长之和除以len
    af = ((list_high - list_low).sum()) / len(list_high)
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

def kdj(date, N=9, M1=3, M2=3):
    datelen = len(date)
    array = np.array(date)
    kdjarr = []
    for i in range(datelen):
        if i - N < 0:
            b = 0
        else:
            b = i - N + 1
        rsvarr = array[b:i + 1, 0:5]
        if (float(max(rsvarr[:, 3])) - float(min(rsvarr[:, 2]))) == 0:
            rsv = -777
        else:
            rsv = (float(rsvarr[-1, 2]) - float(min(rsvarr[:, 4]))) / (float(max(rsvarr[:, 3])) - float(min(rsvarr[:, 2]))) * 100
        if i == 0:
            k = rsv
            d = rsv
        else:
            k = 1 / float(M1) * rsv + (float(M1) - 1) / M1 * float(kdjarr[-1][2])
            d = 1 / float(M2) * k + (float(M2) - 1) / M2 * float(kdjarr[-1][3])
        j = 3 * k - 2 * d
        kdjarr.append(list((rsvarr[-1, 0], rsv, k, d, j)))
    return kdjarr
