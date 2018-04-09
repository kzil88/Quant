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
        temp_day = ts.fund_holdings(year,i+1)
        for j in range(temp_day.shape[0]):
            resu0 = list(temp_day.ix[j])
            print(resu0)
            try:
                sql_insert = "insert into stock_fundholdings(state_dt,report_dt,stock_code,nums,nlast,count,clast,amount,ratio)values('%s','%s','%s','%i','%i','%.2f','%.2f','%.2f','%.2f')"%(str(resu0[2]),str(resu0[2]),str(resu0[0]),int(resu0[3]),int(resu0[4]),float(resu0[5]),float(resu0[6]),float(resu0[7]),float(resu0[8]))
                cursor.execute(sql_insert)
                db.commit()
            except Exception as exp:
                print(exp)
                continue