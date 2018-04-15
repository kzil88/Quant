import datetime
import tushare as ts
import numpy as np
import pymysql
import calendar

if __name__ == '__main__':

    # 建立数据库连接,剔除已入库的部分
    db = pymysql.connect(host='127.0.0.1', user='root', passwd='admin', db='stock', charset='utf8')
    cursor = db.cursor()
    cyear = 2018
    temp_day = ts.xsg_data(year=cyear)
    for j in range(temp_day.shape[0]):
        resu0 = list(temp_day.ix[j])
        print(resu0)
        try:
            sql_insert = "insert into stock_limit(state_dt,stock_code,cnt,ratio)values('%s','%s','%.2f','%.2f')"%(str(resu0[2]),str(resu0[0]),float(resu0[3]),float(resu0[4]))
            cursor.execute(sql_insert)
            db.commit()
        except Exception as exp:
            print(exp)
            continue