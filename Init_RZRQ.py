import datetime
import tushare as ts
import numpy as np
import pymysql
import re

if __name__ == '__main__':
    cons = ts.get_apis()

    # 建立数据库连接,剔除已入库的部分
    db = pymysql.connect(host='127.0.0.1', user='root', passwd='admin', db='stock', charset='utf8')
    cursor = db.cursor()

    try:
        temp_day = ts.sh_margins(start='2016-01-01',end='2018-02-15')
        for i in range(temp_day.shape[0]):
            resu0 = list(temp_day.ix[i])
            print(resu0)
            try:
                sql_insert = "insert into stock_rzrq(state_dt,ctype,rzye,rqye)values('%s','%s','%.2f','%.2f')" % (str(resu0[0]), 'SH', float(resu0[1]), float(resu0[4]))
                cursor.execute(sql_insert)
                db.commit()
            except Exception as exp:
                print(exp)
                continue
    except Exception as ex:
        print(ex)

    try:
        temp_day2 = ts.sz_margins(start='2016-01-01',end='2016-12-31')
        temp_day2mid = np.array(temp_day2)
        for j in range(len(temp_day2mid)):
            resu2 = temp_day2mid[j]
            print(resu2)
            try:
                sql_insert2 = "insert into stock_rzrq(state_dt,ctype,rzye,rqye)values('%s','%s','%.2f','%.2f')" % (str(resu2[0]), 'SZ', float(resu2[2]), float(resu2[5]))
                cursor.execute(sql_insert2)
                db.commit()
            except Exception as exp:
                print(exp)
                continue
    except Exception as ex:
        print(ex)

    try:
        temp_day3 = ts.sz_margins(start='2017-01-01',end='2017-12-31')
        temp_day3mid = np.array(temp_day3)
        for k in range(len(temp_day3mid)):
            resu3 = temp_day3mid[k]
            print(resu3)
            try:
                sql_insert3 = "insert into stock_rzrq(state_dt,ctype,rzye,rqye)values('%s','%s','%.2f','%.2f')" % (str(resu3[0]), 'SZ', float(resu3[2]), float(resu3[5]))
                cursor.execute(sql_insert3)
                db.commit()
            except Exception as exp:
                print(exp)
                continue
    except Exception as ex:
        print(ex)

    print('ALL Finished!!')



