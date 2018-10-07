import datetime
import tushare as ts
import numpy as np
import pymysql
import re


if __name__ == '__main__':
    end_dt = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
    db = pymysql.connect(host='127.0.0.1', user='root', passwd='admin', db='future', charset='utf8')
    cursor = db.cursor()
    cons = ts.get_apis()
    
    future_code = 'RU1805'
    try:
        temp_day = ts.bar(future_code, conn=cons, asset='X', ma=[5, 10, 20, 30, 60],end_date=end_dt)
        c_len = temp_day.shape[0]
    except Exception as aa:
        print('No DATA Code:')
    for j in range(c_len):
        state_dt = str(temp_day.index[c_len-1-j])[0:10]
        resu0 = list(temp_day.ix[c_len-1-j])
        ro = 0.00
        if j != 0:
            resu_base = list(temp_day.ix[c_len-j])
            ro = resu0[2]/resu_base[2]
        resu = []
        for k in range(len(resu0)):
            if str(resu0[k]) == 'nan':
                resu.append(0)
            else:
                resu.append(resu0[k])
        try:
            sql_insert = "INSERT INTO future_all(state_dt,future_code,open,close,high,low,vol,avg,position,ma5,ma10,ma20,ma30,ma60,ro) VALUES ('%s', '%s', '%.2f', '%.2f','%.2f','%.2f','%i','%.2f','%i','%.2f','%.2f','%.2f','%.2f','%.2f','%.4f')" % (state_dt,resu[0],resu[1],resu[2],resu[3],resu[4],resu[5],resu[6],resu[7],resu[8],resu[9],resu[10],resu[11],resu[12],ro)
            cursor.execute(sql_insert)
            db.commit()
            print(str(state_dt) + '   ' + str(resu[0]))
        except Exception as err:
            print('Inner SQL Errrrrr   ' + str(err))
            continue

    print('All Finished!')
    db.close()



