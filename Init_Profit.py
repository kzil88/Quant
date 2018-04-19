import datetime
import tushare as ts
import numpy as np
import pymysql
import calendar

if __name__ == '__main__':

    # 建立数据库连接,剔除已入库的部分
    db = pymysql.connect(host='127.0.0.1', user='root', passwd='admin', db='stock', charset='utf8')
    cursor = db.cursor()
    cyear = 2017
    temp_day = ts.profit_data(year=cyear,top=5000)
    for j in range(temp_day.shape[0]):
        resu0 = list(temp_day.ix[j])
        print(resu0)
        try:
            sql_insert = "insert into stock_profit(state_dt,stock_code,divi,shares)values('%s','%s','%.2f','%i')"%(str(resu0[3]),str(resu0[0]),float(resu0[4]),int(resu0[5]))
            cursor.execute(sql_insert)
            db.commit()
        except Exception as exp:
            print(exp)
            continue