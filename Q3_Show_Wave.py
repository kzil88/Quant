# -*- coding:utf8 -*-
import numpy as np
import time
import talib as ta
import datetime
import HuobiServices as hb
import matplotlib.pyplot as plt
import math
import copy
import tushare as ts
import pymysql

def Signal(coin_symbol,start_dt,end_dt,para_min,para_up,para_low,show_len):
    cons = ts.get_apis()

    temp_day = ts.bar(code=coin_symbol, conn=cons, freq='15min', start_date=start_dt, end_date=end_dt, ma=[5, 10, 20, 30, 60],
                      factors=['vr', 'tor'], adj='qfq')

    print(temp_day)
    amount = []
    vol = []
    close = []
    for i in range(201+show_len):
        temp_list = temp_day.ix[i]
        amount.append(float(temp_list[6]))
        vol.append(float(temp_list[5]))
        close.append(float(temp_list[2]))

    amount = np.array(amount[::-1])
    vol = np.array(vol[::-1])
    close = np.array(close[::-1])
    price_avg_long = []
    price_avg_short = []
    price_close = []
    delt_list = []
    for i in range(201,len(amount)):
        price_avg_long.append(float(amount[i-200:i+1].mean()/vol[i-200:i+1].mean())/100)
        price_avg_short.append(float(amount[i]/vol[i])/100)
        price_close.append(close[i])
        delt_list.append(float(amount[i-200:i+1].mean()/vol[i-200:i+1].mean()) / float(amount[i-1-200:i].mean()/vol[i-1-200:i].mean()))
    return price_avg_long,price_avg_short,price_close,delt_list



if __name__ == '__main__':
    stock_list = ['002008']
    para_list = [(2.5, 2.5), (1.5, 4.5)]
    #para_list = [(4.5, 4), (3.5, 2)]
    para_min = '15min'
    show_len = 200

    db = pymysql.connect(host='127.0.0.1', user='root', passwd='admin', db='stock', charset='utf8')
    cursor = db.cursor()
    end_dt = datetime.datetime.now().strftime('%Y-%m-%d')
    date_seq_len = 100
    sql = "select calendarDate from stock_tradecalall where exchangeCD = 'XSHE' and isOpen = 1 and calendarDate < '%s' order by calendarDate desc limit %s"%(end_dt,date_seq_len)
    cursor.execute(sql)
    done_set = cursor.fetchall()
    date_seq = [x[0] for x in done_set]
    date_seq = date_seq[::-1]

    fig = plt.figure(figsize=(20, 12))
    for i in range(len(stock_list)):
        #signal,resu,color,up_limit,low_limit = Signal(str(coin_list[i])+'usdt',para_min,para_list[i][0],para_list[i][1])
        price_avg_long,price_avg_short,close,delt_list = Signal(str(stock_list[i]),date_seq[0],date_seq[-1],para_min,para_list[i][0],para_list[i][1],show_len)
        a = 221+ i
        ax = fig.add_subplot(a)
        #plt.bar(range(len(resu)), resu)
        plt.plot(range(len(price_avg_long)),price_avg_long,color='blue')
        plt.plot(range(len(price_avg_short)),price_avg_short,color='green')
        plt.plot(range(len(close)),close,color='red')

        # resu_bar = []
        # for j in range(len(price_avg_long)):
        #     resu_bar.append((price_avg_short[j]-price_avg_long[j])*delt_list[j])
        ax2 = fig.add_subplot(a+2)
        plt.bar(range(len(price_avg_long)), np.array(price_avg_short)-np.array(price_avg_long))
        #plt.bar(range(len(price_avg_long)), np.array(price_avg_short) - np.array(close))
        #plt.bar(range(len(price_avg_long)), resu_bar)

    plt.show()

