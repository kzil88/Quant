import datetime
import tushare as ts
import numpy as np
import pymysql
import calendar

if __name__ == '__main__':

    # 建立数据库连接,剔除已入库的部分
    db = pymysql.connect(host='127.0.0.1', user='root', passwd='admin', db='stock', charset='utf8')
    cursor = db.cursor()
    year = 2017
    for i in range(4):
        temp_day = ts.forecast_data(year,i+1)
        for j in range(temp_day.shape[0]):
            resu0 = list(temp_day.ix[j])
            print(resu0)
            try:
                c_range = str(resu0[5])
                c_range = c_range.replace('~','to')
                c_range = c_range.replace('%','p')
                c_range = c_range.replace('-','neg')
                print(c_range)
                sql_insert = "insert into stock_forecast(state_dt,stock_code,type,pre_eps,c_range)values('%s','%s','%s','%.2f','%s')"%(str(resu0[3]),str(resu0[0]),str(resu0[2]),float(resu0[4]),str(resu0[5]))
                cursor.execute(sql_insert)
                db.commit()
            except Exception as exp:
                print(exp)
                continue