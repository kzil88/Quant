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

    def __init__(self, in_code,start_dt,end_dt):
        self.collectDATA(in_code,start_dt,end_dt)

    def collectDATA(self,in_code,start_dt,end_dt):
        # 建立数据库连接,剔除已入库的部分
        db = pymysql.connect(host='127.0.0.1', user='root', passwd='admin', db='stock', charset='utf8')
        cursor = db.cursor()
        try:
            sql_done_set = "SELECT * FROM stock_price_day_list a where stock_code = '%s' and state_dt >= '%s' and state_dt <= '%s' order by state_dt asc" % (
            in_code, start_dt, end_dt)
            cursor.execute(sql_done_set)
            done_set = cursor.fetchall()
            
            if len(done_set) == 0:
                raise Exception
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

        except Exception as excp:
            print(excp)
        db.close()

        if len(self.close_list) > 0:
            self.open_list = np.array(self.open_list)
            self.close_list = np.array(self.close_list)
            self.high_list = np.array(self.high_list)
            self.low_list = np.array(self.low_list)
            self.code = in_code
            period = min(20,len(self.close_list))
            self.avg = ta.MA(self.close_list, period)
            self.avg = np.array([x for x in self.avg if str(x) != 'nan'])
            self.good_buy = np.array([x * (1.00 - self.good_factor) for x in self.avg])
            self.good_sell = np.array([x * (1.00 + self.good_factor) for x in self.avg])
            self.bad_sell = np.array([x * (1.00 - self.bad_factor) for x in self.avg])
            self.cnt_risk = [0] * len(self.avg)
            self.cnt_good_sell = [0] * len(self.avg)
            self.cnt_good_buy = [0] * len(self.avg)
            self.cnt_bad_sell = [0] * len(self.avg)
            # for a in range(len(self.avg)):
            #     self.cnt_bad_sell[a] = len([x for x in self.low_list[:a + period - 1] if x <= self.bad_sell[a]])
            #     self.cnt_good_sell[a] = len([x for x in self.high_list[:a + period - 1] if x >= self.good_sell[a]])
            #     self.cnt_bad_sell[a] = len([x for x in self.low_list[:a + period - 1] if self.bad_sell[a] < x <= self.good_buy[a]])
            #     self.cnt_risk[a] = len([x for x in self.low_list[:a + period - 1] if x <= self.close_list[a + period - 1]])

            # ARFQ
            for b in range(len(self.avg)):
                af, fq, process = get_arfq(np.array(self.close_list[b:b + period]), np.array(self.close_list[b:b + period]),self.good_sell[b], self.bad_sell[b], self.good_buy[b])
                af2 = ((np.array(self.high_list[b:b+period]) - np.array(self.low_list[b:b+period])).sum())/(period)
                self.af.append(af2)
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

def choose_main(start_dt,end_dt):
    # 筛选窗口20天
    time_temp = datetime.datetime.strptime(end_dt,'%Y-%m-%d') - datetime.timedelta(days=30)
    new_start_dt = time_temp.strftime('%Y-%m-%d')
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
    sql_done_set = "SELECT distinct stock_code FROM q2_cs where calc_date = '%s'" % (end_dt)
    try:
        cursor.execute(sql_done_set)
        done_set = cursor.fetchall()
        for row in done_set:
            code_set.remove(row[0])
            print('Romove  ' + str(row[0]))
        cnt = 1
        total = len(code_set)
        for i in code_set:
            try:
                cs = CS(i,new_start_dt,end_dt)
                if len(cs.close_list) == 0:
                    raise Exception
                sql = "INSERT INTO q2_cs(stock_code,af,freq,process,calc_date) VALUES ('%s', '%f', '%f','%f','%s')" % (i, cs.af[-1], cs.fq[-1], cs.process[-1],end_dt)
                cursor.execute(sql)
                db.commit()
                cnt += 1
                print('Date: '+str(end_dt)+'  Seq: '+str(cnt)+' of ' + str(total) +'   Code: ' + str(i) + '   AF: ' + str(cs.af[-1]) + '    Freq: ' + str(cs.fq[-1]) + '    Process: ' + str(cs.process[-1]))
            except Exception as excp:
                # db.rollback()
                print(str('Errr') + str(i))
        print('All Finished!')

        for j in range(3):
            try:
                sql_check = "select * from q2_cs a order by a.freq desc limit 100"
                cursor.execute(sql_check)
                done_set2 = cursor.fetchall()
                cnt = 1
                for row in done_set2:
                    sql_del = "delete from q2_cs where stock_code = '%s'"%(row[0])
                    cursor.execute(sql_del)
                    db.commit()
                    new_cs = CS(row[0],new_start_dt,end_dt)
                    if len(new_cs.close_list) == 0:
                        raise Exception
                    sql_check_insert = "INSERT INTO q2_cs(stock_code,af,freq,process,calc_date) VALUES ('%s', '%f', '%f','%f','%s')" % (row[0], new_cs.af[-1], new_cs.fq[-1], new_cs.process[-1], end_dt)
                    cursor.execute(sql_check_insert)
                    db.commit()
                    cnt += 1
                    print('Date: ' + str(end_dt) + '   Echo:  ' + str(j) + '  Seq: ' + str(cnt) + ' of ' + str(len(done_set2)) + '   Code: ' + str(row[0]) + '   AF: ' + str(new_cs.af[-1]) + '    Freq: ' + str(new_cs.fq[-1]) + '    Process: ' + str(new_cs.process[-1]))
            except Exception as ex:
                print('Echo Part Errrrr   ' + str(ex))


    except Exception as excp:
        #db.rollback()
        print(str('Errr')+ str(i)+str(excp))

    db.close()

if __name__ == '__main__':
    end_dt = '2017-10-30'
    time_temp = datetime.datetime.strptime('2017-10-30', '%Y-%m-%d') - datetime.timedelta(days=30)
    new_start_dt = time_temp.strftime('%Y-%m-%d')

    # cs = CS('600265',new_start_dt , '2017-10-30')
    # print('XXX')
    db = pymysql.connect(host='127.0.0.1', user='root', passwd='admin', db='stock', charset='utf8')
    cursor = db.cursor()
    for j in range(3):
        try:
            sql_check = "select * from q2_cs a order by a.freq desc limit 100"
            cursor.execute(sql_check)
            done_set2 = cursor.fetchall()
            cnt = 1
            for row in done_set2:
                sql_del = "delete from q2_cs where stock_code = '%s'" % (row[0])
                cursor.execute(sql_del)
                db.commit()
                new_cs = CS(row[0], new_start_dt, end_dt)
                if len(new_cs.close_list) == 0:
                    raise Exception
                sql_check_insert = "INSERT INTO q2_cs(stock_code,af,freq,process,calc_date) VALUES ('%s', '%f', '%f','%f','%s')" % (
                row[0], new_cs.af[-1], new_cs.fq[-1], new_cs.process[-1], end_dt)
                cursor.execute(sql_check_insert)
                db.commit()
                print('Date: ' + str(end_dt) + '   Echo:  ' + str(j) + '  Seq: ' + str(cnt) + ' of ' + str(len(done_set2)) + '   Code: ' + str(row[0]) + '   AF: ' + str(new_cs.af[-1]) + '    Freq: ' + str(new_cs.fq[-1]) + '    Process: ' + str(new_cs.process[-1]))
                cnt += 1
        except Exception as ex:
            print('Echo Part Errrrr   ' + str(ex))
