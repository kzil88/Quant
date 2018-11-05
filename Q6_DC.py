# -*- coding:utf8 -*-
import tushare as ts
import re
import numpy as np
from pandas import DataFrame
import matplotlib.pyplot as plt
import pandas as pd
import datetime
import talib as ta
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

    def __init__(self, in_code,start_dt,end_dt,year):
        self.collectDATA(in_code,start_dt,end_dt,year)

    def collectDATA(self,in_code,start_dt,end_dt,year):
        # 建立数据库连接,剔除已入库的部分
        db = pymysql.connect(host='127.0.0.1', user='root', passwd='admin', db='stock', charset='utf8')
        cursor = db.cursor()
        sql_done_set = "SELECT * FROM stock_%s a where stock_code = '%s' and state_dt >= '%s' and state_dt <= '%s' order by state_dt asc" % (str(year),in_code, start_dt,end_dt)
        cursor.execute(sql_done_set)
        done_set = cursor.fetchall()
        if len(done_set) == 0:
            raise Exception
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
        for i in range(len(done_set)):
            self.date_seq.append(done_set[i][0])
            self.open_list.append(float(done_set[i][2]))
            self.close_list.append(float(done_set[i][3]))
            self.high_list.append(float(done_set[i][4]))
            self.low_list.append(float(done_set[i][5]))
            self.vol_list.append(float(done_set[i][6]))
            self.amount_list.append(float(done_set[i][7]))
            self.tor_list.append(float(done_set[i][8]))
            self.vr_list.append(float(done_set[i][9]))
            self.ma5_list.append(float(done_set[i][10]))
            self.ma10_list.append(float(done_set[i][11]))
            self.ma20_list.append(float(done_set[i][12]))
            self.ma30_list.append(float(done_set[i][13]))
            self.ma60_list.append(float(done_set[i][14]))
        db.close()

        cdl_2crows = ta.CDL2CROWS(np.array(self.open_list),np.array(self.high_list),np.array(self.low_list),np.array(self.close_list))
        cdl_3blackcrows = ta.CDL3BLACKCROWS(np.array(self.open_list),np.array(self.high_list),np.array(self.low_list),np.array(self.close_list))
        cdl_3inside = ta.CDL3INSIDE(np.array(self.open_list), np.array(self.high_list), np.array(self.low_list),np.array(self.close_list))
        cdl_3linestrike = ta.CDL3LINESTRIKE(np.array(self.open_list),np.array(self.high_list),np.array(self.low_list),np.array(self.close_list))
        cdl_3outside = ta.CDL3OUTSIDE(np.array(self.open_list), np.array(self.high_list), np.array(self.low_list),np.array(self.close_list))
        cdl_3starsinsouth = ta.CDL3STARSINSOUTH(np.array(self.open_list), np.array(self.high_list), np.array(self.low_list),np.array(self.close_list))
        cdl_3whitesoldiers = ta.CDL3WHITESOLDIERS(np.array(self.open_list), np.array(self.high_list),np.array(self.low_list),np.array(self.close_list))
        cdl_abandonedbaby = ta.CDLABANDONEDBABY(np.array(self.open_list), np.array(self.high_list),np.array(self.low_list),np.array(self.close_list))
        cdl_advancedblock = ta.CDLADVANCEBLOCK(np.array(self.open_list), np.array(self.high_list),np.array(self.low_list),np.array(self.close_list))
        cdl_belthold = ta.CDLBELTHOLD(np.array(self.open_list), np.array(self.high_list),np.array(self.low_list),np.array(self.close_list))
        cdl_breakaway = ta.CDLBREAKAWAY(np.array(self.open_list), np.array(self.high_list),np.array(self.low_list),np.array(self.close_list))
        cdl_closing = ta.CDLCLOSINGMARUBOZU(np.array(self.open_list), np.array(self.high_list),np.array(self.low_list),np.array(self.close_list))
        cdl_conbaby = ta.CDLCONCEALBABYSWALL(np.array(self.open_list), np.array(self.high_list),np.array(self.low_list),np.array(self.close_list))
        cdl_counterattack = ta.CDLCOUNTERATTACK(np.array(self.open_list), np.array(self.high_list),np.array(self.low_list),np.array(self.close_list))
        cdl_darkcloud = ta.CDLDARKCLOUDCOVER(np.array(self.open_list), np.array(self.high_list),np.array(self.low_list),np.array(self.close_list))
        cdl_doji = ta.CDLDOJI(np.array(self.open_list), np.array(self.high_list),np.array(self.low_list),np.array(self.close_list))
        cdl_dojistar = ta.CDLDOJISTAR(np.array(self.open_list), np.array(self.high_list),np.array(self.low_list),np.array(self.close_list))
        cdl_dragondoji = ta.CDLDRAGONFLYDOJI(np.array(self.open_list), np.array(self.high_list),np.array(self.low_list),np.array(self.close_list))
        cdl_eng = ta.CDLENGULFING(np.array(self.open_list), np.array(self.high_list),np.array(self.low_list),np.array(self.close_list))
        cdl_evedoji = ta.CDLEVENINGDOJISTAR(np.array(self.open_list), np.array(self.high_list),np.array(self.low_list),np.array(self.close_list))
        cdl_evestar = ta.CDLEVENINGSTAR(np.array(self.open_list), np.array(self.high_list),np.array(self.low_list),np.array(self.close_list))
        cdl_gapside = ta.CDLGAPSIDESIDEWHITE(np.array(self.open_list), np.array(self.high_list),np.array(self.low_list),np.array(self.close_list))
        cdl_gravedoji = ta.CDLGRAVESTONEDOJI(np.array(self.open_list), np.array(self.high_list),np.array(self.low_list),np.array(self.close_list))
        cdl_hammer = ta.CDLHAMMER(np.array(self.open_list), np.array(self.high_list),np.array(self.low_list),np.array(self.close_list))
        cdl_hanging = ta.CDLHANGINGMAN(np.array(self.open_list), np.array(self.high_list),np.array(self.low_list),np.array(self.close_list))
        cdl_hara = ta.CDLHARAMI(np.array(self.open_list), np.array(self.high_list),np.array(self.low_list),np.array(self.close_list))
        cdl_haracross = ta.CDLHARAMICROSS(np.array(self.open_list), np.array(self.high_list),np.array(self.low_list),np.array(self.close_list))
        cdl_highwave = ta.CDLHIGHWAVE(np.array(self.open_list), np.array(self.high_list),np.array(self.low_list),np.array(self.close_list))
        cdl_hikk = ta.CDLHIKKAKE(np.array(self.open_list), np.array(self.high_list),np.array(self.low_list),np.array(self.close_list))
        cdl_hikkmod = ta.CDLHIKKAKEMOD(np.array(self.open_list), np.array(self.high_list),np.array(self.low_list),np.array(self.close_list))
        cdl_homing = ta.CDLHOMINGPIGEON(np.array(self.open_list), np.array(self.high_list),np.array(self.low_list),np.array(self.close_list))
        cdl_i3crows = ta.CDLIDENTICAL3CROWS(np.array(self.open_list), np.array(self.high_list),np.array(self.low_list),np.array(self.close_list))
        cdl_inneck = ta.CDLINNECK(np.array(self.open_list), np.array(self.high_list),np.array(self.low_list),np.array(self.close_list))
        cdl_inverhammer = ta.CDLINVERTEDHAMMER(np.array(self.open_list), np.array(self.high_list),np.array(self.low_list),np.array(self.close_list))
        cdl_kicking = ta.CDLKICKING(np.array(self.open_list), np.array(self.high_list),np.array(self.low_list),np.array(self.close_list))
        cdl_kicking2 = ta.CDLKICKINGBYLENGTH(np.array(self.open_list), np.array(self.high_list),np.array(self.low_list),np.array(self.close_list))
        cdl_ladder = ta.CDLLADDERBOTTOM(np.array(self.open_list), np.array(self.high_list),np.array(self.low_list),np.array(self.close_list))
        cdl_longdoji = ta.CDLLONGLEGGEDDOJI(np.array(self.open_list), np.array(self.high_list),np.array(self.low_list),np.array(self.close_list))
        cdl_longline = ta.CDLLONGLINE(np.array(self.open_list), np.array(self.high_list),np.array(self.low_list),np.array(self.close_list))
        cdl_marubo = ta.CDLMARUBOZU(np.array(self.open_list), np.array(self.high_list),np.array(self.low_list),np.array(self.close_list))
        cdl_matchinglow = ta.CDLMATCHINGLOW(np.array(self.open_list), np.array(self.high_list),np.array(self.low_list),np.array(self.close_list))
        cdl_mathold = ta.CDLMATHOLD(np.array(self.open_list), np.array(self.high_list),np.array(self.low_list),np.array(self.close_list))
        cdl_morningdoji = ta.CDLMORNINGDOJISTAR(np.array(self.open_list), np.array(self.high_list),np.array(self.low_list),np.array(self.close_list))
        cdl_morningstar = ta.CDLMORNINGSTAR(np.array(self.open_list), np.array(self.high_list),np.array(self.low_list),np.array(self.close_list))
        cdl_onneck = ta.CDLONNECK(np.array(self.open_list), np.array(self.high_list),np.array(self.low_list),np.array(self.close_list))
        cdl_pier = ta.CDLPIERCING(np.array(self.open_list), np.array(self.high_list),np.array(self.low_list),np.array(self.close_list))
        cdl_rick = ta.CDLRICKSHAWMAN(np.array(self.open_list), np.array(self.high_list),np.array(self.low_list),np.array(self.close_list))
        cdl_3methords = ta.CDLRISEFALL3METHODS(np.array(self.open_list), np.array(self.high_list),np.array(self.low_list),np.array(self.close_list))
        cdl_seprate = ta.CDLSEPARATINGLINES(np.array(self.open_list), np.array(self.high_list),np.array(self.low_list),np.array(self.close_list))
        cdl_shoot = ta.CDLSHOOTINGSTAR(np.array(self.open_list), np.array(self.high_list),np.array(self.low_list),np.array(self.close_list))
        cdl_shortcandle = ta.CDLSHORTLINE(np.array(self.open_list), np.array(self.high_list),np.array(self.low_list),np.array(self.close_list))
        cdl_spin = ta.CDLSPINNINGTOP(np.array(self.open_list), np.array(self.high_list),np.array(self.low_list),np.array(self.close_list))
        cdl_stalled = ta.CDLSTALLEDPATTERN(np.array(self.open_list), np.array(self.high_list),np.array(self.low_list),np.array(self.close_list))
        cdl_sandwich = ta.CDLSTICKSANDWICH(np.array(self.open_list), np.array(self.high_list),np.array(self.low_list),np.array(self.close_list))
        cdl_taku = ta.CDLTAKURI(np.array(self.open_list), np.array(self.high_list),np.array(self.low_list),np.array(self.close_list))
        cdl_takugap = ta.CDLTASUKIGAP(np.array(self.open_list), np.array(self.high_list),np.array(self.low_list),np.array(self.close_list))
        cdl_thrust = ta.CDLTHRUSTING(np.array(self.open_list), np.array(self.high_list),np.array(self.low_list),np.array(self.close_list))
        cdl_tristar = ta.CDLTRISTAR(np.array(self.open_list), np.array(self.high_list),np.array(self.low_list),np.array(self.close_list))
        cdl_uni = ta.CDLUNIQUE3RIVER(np.array(self.open_list), np.array(self.high_list),np.array(self.low_list),np.array(self.close_list))
        cdl_upgap = ta.CDLUPSIDEGAP2CROWS(np.array(self.open_list), np.array(self.high_list),np.array(self.low_list),np.array(self.close_list))
        cdl_xside = ta.CDLXSIDEGAP3METHODS(np.array(self.open_list), np.array(self.high_list),np.array(self.low_list),np.array(self.close_list))

        self.data_train = []
        self.data_target = []
        self.data_target_onehot = []
        for i in range(len(self.close_list)-5):
            train = [cdl_2crows[i],cdl_3blackcrows[i],cdl_3inside[i],cdl_3linestrike[i],cdl_3outside[i],cdl_3starsinsouth[i],cdl_3whitesoldiers[i],cdl_abandonedbaby[i],cdl_advancedblock[i],cdl_belthold[i],cdl_breakaway[i],cdl_closing[i],
                     cdl_conbaby[i],cdl_counterattack[i],cdl_darkcloud[i],cdl_doji[i],cdl_dojistar[i],cdl_dragondoji[i],cdl_eng[i],cdl_evedoji[i],cdl_evestar[i],cdl_gapside[i],
                     cdl_gravedoji[i],cdl_hammer[i],cdl_hanging[i],cdl_hara[i],cdl_haracross[i],cdl_highwave[i],cdl_hikk[i],cdl_hikkmod[i],cdl_homing[i],cdl_i3crows[i],cdl_inneck[i],
                     cdl_inverhammer[i],cdl_kicking[i],cdl_kicking2[i],cdl_ladder[i],cdl_longdoji[i],cdl_longline[i],cdl_marubo[i],cdl_matchinglow[i],cdl_mathold[i],cdl_morningdoji[i],
                     cdl_morningstar[i],cdl_onneck[i],cdl_pier[i],cdl_rick[i],cdl_3methords[i],cdl_seprate[i],cdl_shoot[i],cdl_shortcandle[i],cdl_spin[i],cdl_stalled[i],cdl_sandwich[i],cdl_taku[i],
                     cdl_takugap[i],cdl_thrust[i],cdl_tristar[i],cdl_uni[i],cdl_upgap[i],cdl_xside[i]]
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
            [cdl_2crows[-1], cdl_3blackcrows[-1], cdl_3inside[-1], cdl_3linestrike[-1], cdl_3outside[-1],
             cdl_3starsinsouth[-1], cdl_3whitesoldiers[-1], cdl_abandonedbaby[-1], cdl_advancedblock[-1], cdl_belthold[-1],
             cdl_breakaway[-1], cdl_closing[-1],
             cdl_conbaby[-1], cdl_counterattack[-1], cdl_darkcloud[-1], cdl_doji[-1], cdl_dojistar[-1], cdl_dragondoji[-1],
             cdl_eng[-1], cdl_evedoji[-1], cdl_evestar[-1], cdl_gapside[-1],
             cdl_gravedoji[-1], cdl_hammer[-1], cdl_hanging[-1], cdl_hara[-1], cdl_haracross[-1], cdl_highwave[-1],
             cdl_hikk[-1], cdl_hikkmod[-1], cdl_homing[-1], cdl_i3crows[-1], cdl_inneck[-1],
             cdl_inverhammer[-1], cdl_kicking[-1], cdl_kicking2[-1], cdl_ladder[-1], cdl_longdoji[-1], cdl_longline[-1],
             cdl_marubo[-1], cdl_matchinglow[-1], cdl_mathold[-1], cdl_morningdoji[-1],
             cdl_morningstar[-1], cdl_onneck[-1], cdl_pier[-1], cdl_rick[-1], cdl_3methords[-1], cdl_seprate[-1],
             cdl_shoot[-1], cdl_shortcandle[-1], cdl_spin[-1], cdl_stalled[-1], cdl_sandwich[-1], cdl_taku[-1],
             cdl_takugap[-1], cdl_thrust[-1], cdl_tristar[-1], cdl_uni[-1], cdl_upgap[-1], cdl_xside[-1]]
        )
        self.data_train = np.array(self.data_train)
        self.data_target = np.array(self.data_target)

