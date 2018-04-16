import datetime
import tushare as ts
import numpy as np
import pymysql
import re

if __name__ == '__main__':


    # 建测试时间序列（筛选出交易日序列）
    cons = ts.get_apis()
    start_dt = '2016-01-04'
    time_temp = datetime.datetime.now() - datetime.timedelta(days=10)
    end_dt = time_temp.strftime('%Y-%m-%d')


    # 建立数据库连接,剔除已入库的部分
    db = pymysql.connect(host='127.0.0.1', user='root', passwd='admin', db='stock', charset='utf8')
    cursor = db.cursor()
    # 建时间序列
    sql_done_set = "select calendarDate from stock_tradecalall a where a.calendarDate >= '%s' and a.calendarDate < '%s' and a.exchangeCD = 'XSHG' and isOpen = 1 order by a.calendarDate asc" % (start_dt, end_dt)
    cursor.execute(sql_done_set)
    done_set = cursor.fetchall()
    db.commit()
    date_seq = [x[0] for x in done_set]
    # 股票池
    sql_select = "SELECT distinct stock_code FROM stock_all"
    cursor.execute(sql_select)
    done_set2 = cursor.fetchall()
    db.commit()
    stock_pool = [x[0] for x in done_set2]
    cnt = 0
    for stock in stock_pool:
        total = len(stock_pool)
        cnt += 1
        for i in range(len(date_seq)-3):
            sql_inner_select = "select * from stock_all a where a.state_dt >= '%s' and a.state_dt <= '%s' and a.stock_code = '%s' order by a.state_dt asc"%(date_seq[i],date_seq[i+3],stock)
            cursor.execute(sql_inner_select)
            done_set3 = cursor.fetchall()
            if len(done_set3) < 4:
                continue
            delt = done_set3[3][3]/done_set3[0][3] - 1
            resu5t10 = 0
            resu10t15 = 0
            resu15up = 0
            if 0.05 <= delt < 0.1:
                resu5t10 = 1
            elif 0.1<= delt < 0.15:
                resu10t15 = 1
            elif delt >= 0.15:
                resu15up = 1
            if delt >=0.05:
                sql_insert = "insert into lt(state_dt,stock_code,z5t10,z10t15,z15up,delt,bz)values('%s','%s','%.4f','%.4f','%.4f','%.4f','%s')"%(date_seq[i],stock,float(resu5t10),float(resu10t15),float(resu15up),float(delt),str('3days'))
                cursor.execute(sql_insert)
                db.commit()
        print('Stock_code : ' + str(stock) + '   ' + str(cnt) + '  of  ' + str(total))
    print('ALL Finished！！')
    db.close()


