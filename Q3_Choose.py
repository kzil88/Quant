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


def q3_ana(sql,layer,start_dt,end_dt):
    db = pymysql.connect(host='127.0.0.1', user='root', passwd='admin', db='stock', charset='utf8')
    cursor = db.cursor()
    cursor.execute(sql)
    done_set3 = cursor.fetchall()
    s1_resu = []
    if len(done_set3) > 0:
        s1_pool = [x[1] for x in done_set3]
        for stock_j in s1_pool:
            sql_select2 = "select * from stock_price_day_list a where a.stock_code = '%s' and a.state_dt >= '%s' and a.state_dt <= '%s'" % (stock_j, start_dt, end_dt)
            cursor.execute(sql_select2)
            done_set4 = cursor.fetchall()
            if len(done_set4) > 0:
                base_price = done_set4[0][3]
                compare_list = [x[3] for x in done_set4 if x[0] != done_set4[0][0]]
                if len(compare_list) > 0:
                    profit_max = (max(compare_list) / base_price) - 1
                    profit_min = (min(compare_list) / base_price) - 1
                    s1_resu.append(profit_max)
        if len(s1_resu) > 0:
            cnt_win = len([x for x in s1_resu if x > 0])
            cnt_total = len(s1_resu)
            cnt_win_ro = cnt_win / cnt_total
            profit_sum = sum(s1_resu)
            sql_insert = "insert into q3_ana(state_dt,slayer,cnt_win,cnt_total,cnt_win_ro,profit_sum)values('%s','%i','%i','%i','%.2f','%.4f')" % (start_dt, layer, cnt_win, cnt_total, cnt_win_ro, profit_sum)
            cursor.execute(sql_insert)
            db.commit()
            #print(str(start_dt) + '   SLayer : ' + str(layer) +'   cnt_win : ' + str(cnt_win) + '   cnt_total : ' + str(cnt_total) + '   cnt_win_ro : ' + str(cnt_win_ro) + '   profit_sum : ' + str(profit_sum))
    return 1


def ChooseMain(index_date):
    time_temp = datetime.datetime.strptime(index_date,'%Y-%m-%d') - datetime.timedelta(days=90)
    date_seq_start = time_temp.strftime('%Y-%m-%d')
    # 建立数据库连接,回测时间序列
    db = pymysql.connect(host='127.0.0.1', user='root', passwd='admin', db='stock', charset='utf8')
    cursor = db.cursor()
    sql_done_set = "select prevTradeDate from stock_tradecalall a where a.calendarDate >= '%s' and a.calendarDate <= '%s' and a.exchangeCD = 'XSHG' and isOpen = 1 order by a.prevTradeDate asc" % (date_seq_start,index_date)
    cursor.execute(sql_done_set)
    done_set = cursor.fetchall()
    date_seq = [x[0] for x in done_set]

    # 窗口时间序列 5-days，第一天看涨幅，后四天是超额收益
    sql_truncate = "truncate table q3_ana"
    cursor.execute(sql_truncate)
    db.commit()
    sql_stock_all = "select distinct stock_code from stock_price_day_list a"
    cursor.execute(sql_stock_all)
    done_set2 = cursor.fetchall()
    stock_pool = [x[0] for x in done_set2]
    for i in range(len(date_seq)-4):
        # 策略一: 日涨幅超3% 且收盘小于ma5,大于其他
        sql_select = "select * from stock_price_day_list a where a.ro > 1.03 and a.close < ma5 and a.close > ma10 and a.close > ma20 and a.close > ma30 and a.close > ma60  and a.state_dt = '%s'"%(date_seq[i])
        ans = q3_ana(sql_select,1,date_seq[i],date_seq[i+4])

        # 策略二: 日涨幅超3% 且收盘小于ma5,ma10; 大于其他
        sql_select_s2 = "select * from stock_price_day_list a where a.ro > 1.03 and a.close < ma5 and a.close < ma10 and a.close > ma20 and a.close > ma30 and a.close > ma60  and a.state_dt = '%s'" % (date_seq[i])
        ans = q3_ana(sql_select_s2,2,date_seq[i],date_seq[i+4])

        # 策略三: 日涨幅超3% 且收盘小于ma5,ma10,ma20; 大于其他
        sql_select_s3 = "select * from stock_price_day_list a where a.ro > 1.03 and a.close < ma5 and a.close < ma10 and a.close < ma20 and a.close > ma30 and a.close > ma60  and a.state_dt = '%s'" % (date_seq[i])
        ans = q3_ana(sql_select_s3,3,date_seq[i],date_seq[i+4])

        # 策略四: 日涨幅超3% 且收盘小于ma5,ma10,ma20,ma30; 大于其他
        sql_select_s4 = "select * from stock_price_day_list a where a.ro > 1.03 and a.close < ma5 and a.close < ma10 and a.close < ma20 and a.close < ma30 and a.close > ma60  and a.state_dt = '%s'" % (date_seq[i])
        ans = q3_ana(sql_select_s4,4,date_seq[i],date_seq[i+4])

        # 策略五: 日涨幅超3% 且收盘小于ma5,ma10,ma20,ma30,ma60
        sql_select_s5 = "select * from stock_price_day_list a where a.ro > 1.03 and a.close < ma5 and a.close < ma10 and a.close < ma20 and a.close < ma30 and a.close < ma60  and a.state_dt = '%s'" % (date_seq[i])
        ans = q3_ana(sql_select_s5,5,date_seq[i],date_seq[i+4])
    db.close()
    db = pymysql.connect(host='127.0.0.1', user='root', passwd='admin', db='stock', charset='utf8')
    cursor = db.cursor()
    sql_group = "select slayer,sum(profit_sum) ans from q3_ana group by slayer order by slayer"
    cursor.execute(sql_group)
    done_set_group  = cursor.fetchall()
    cut_dt = date_seq[-20]
    sql_group2 = "select slayer,sum(profit_sum) ans from q3_ana where state_dt >= '%s' group by slayer order by slayer"%(cut_dt)
    cursor.execute(sql_group2)
    done_set_group2 = cursor.fetchall()
    score1 = score2 = score3 = score4 = score5 = 0
    for k in range(len(done_set_group)):
        if done_set_group[k][0] == 1:
            score1 = score1 + float(done_set_group[k][1])*0.4
        elif done_set_group[k][0] == 2:
            score2 = score2 + float(done_set_group[k][1])*0.4
        elif done_set_group[k][0] == 3:
            score3 = score3 + float(done_set_group[k][1])*0.4
        elif done_set_group[k][0] == 4:
            score4 = score4 + float(done_set_group[k][1])*0.4
        elif done_set_group[k][0] == 5:
            score5 = score5 + float(done_set_group[k][1])*0.4
    for j in range(len(done_set_group2)):
        if done_set_group2[j][0] == 1:
            score1 = score1 + float(done_set_group2[j][1])*0.6
        elif done_set_group2[j][0] == 2:
            score2 = score2 + float(done_set_group2[j][1])*0.6
        elif done_set_group2[j][0] == 3:
            score3 = score3 + float(done_set_group2[j][1])*0.6
        elif done_set_group2[j][0] == 4:
            score4 = score4 + float(done_set_group2[j][1])*0.6
        elif done_set_group2[j][0] == 5:
            score5 = score5 + float(done_set_group2[j][1])*0.6
    score_merge = [score1,score2,score3,score4,score5]
    max_score = max(score_merge)
    resu_s = 0
    for l in range(len(score_merge)):
        if score_merge[l] == max_score:
            resu_s = l+1
            break
    return resu_s