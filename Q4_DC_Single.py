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
import pymysql


class data_collect2(object):
    code = ''
    date_seq = []
    open_list = []
    close_list = []
    high_list = []
    low_list = []
    vol_list = []
    amount_list = []
    tor_list = []
    vr_list = []
    ma5_list = []
    ma10_list = []
    ma20_list = []
    ma30_list = []
    ma60_list = []
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
    test_case = []
    cnt_pos = 0

    def __init__(self, in_code,start_dt,end_dt):
        self.collectDATA(in_code,start_dt,end_dt)

    def collectDATA(self,in_code,start_dt,end_dt):

        self.date_seq = []
        self.open_list = []
        self.close_list = []
        self.high_list = []
        self.low_list = []
        self.vol_list = []
        self.amount_list = []
        self.tor_list = []
        self.vr_list = []
        self.ma5_list = []
        self.ma10_list = []
        self.ma20_list = []
        self.ma30_list = []
        self.ma60_list = []

        cons = ts.get_apis()
        temp_day = ts.bar(code=in_code, conn=cons, freq='D', start_date=start_dt, end_date=end_dt, ma=[5, 10, 20, 30, 60],
                          factors=['vr', 'tor'], adj='qfq')
        print(temp_day)
        c_len = temp_day.shape[0]
        for j in range(c_len):
            state_dt = str(temp_day.index[c_len - 1 - j])[0:10]
            resu0 = list(temp_day.ix[c_len - 1 - j])
            ro = 0.00
            if j != 0:
                resu_base = list(temp_day.ix[c_len - j])
                ro = resu0[2] / resu_base[2]
            resu = []
            for k in range(len(resu0)):
                if str(resu0[k]) == 'nan':
                    resu.append(0)
                else:
                    resu.append(resu0[k])

            self.date_seq.append(state_dt)
            self.open_list.append(float(resu[1]))
            self.close_list.append(float(resu[2]))
            self.high_list.append(float(resu[3]))
            self.low_list.append(float(resu[4]))
            self.vol_list.append(float(resu[5]))
            self.amount_list.append(float(resu[6]))
            self.tor_list.append(float(resu[7]))
            self.vr_list.append(float(resu[8]))
            self.ma5_list.append(float(resu[9]))
            self.ma10_list.append(float(resu[10]))
            self.ma20_list.append(float(resu[11]))
            self.ma30_list.append(float(resu[12]))
            self.ma60_list.append(float(resu[13]))

        print(self.date_seq)
        print(self.open_list)




        self.data_train = []
        self.data_target = []
        self.data_target_onehot = []
        for i in range(len(self.close_list)-5):
            train = []
            self.data_train.append(np.array(train))
            # after_max_price = max(self.close_list[i+1:i + 5])
            # after_min_price = min(self.close_list[i+1:i+5])
            # if after_max_price / self.close_list[i] >= 1.01:
            #     self.data_target.append(float(1.00))
            #     self.data_target_onehot.append([1,0,0])
            # elif after_min_price / self.close_list[i] < 0.99:
            #     self.data_target.append(float(-1.00))
            #     self.data_target_onehot.append([0,1,0])
            # else:
            #     self.data_target.append(float(0.00))
            #     self.data_target_onehot.append([0,0,1])

            after_mean_price = np.array(self.close_list[i+1:i+5]).mean()
            if after_mean_price/self.close_list[i] > 1.03:
                self.data_target.append(float(1.00))
                self.data_target_onehot.append([1,0,0])
            else:
                self.data_target.append(float(-1.00))
                self.data_target_onehot.append([0,1,0])
        self.cnt_pos = 0
        self.cnt_pos =len([x for x in self.data_target if x == 1.00])

        self.test_case = []
        self.test_case = np.array(
            []
        )
        self.data_train = np.array(self.data_train)
        self.data_target = np.array(self.data_target)

