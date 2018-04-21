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

    sql_date = "select prevTradeDate from stock_tradecalall a where a.calendarDate >= '%s' and a.exchangeCD = 'XSHG' and isOpen = 1 order by a.prevTradeDate asc" % (
        '2016-01-01')
    cursor.execute(sql_date)
    done_set_date = cursor.fetchall()
    date_seq = [x[0] for x in done_set_date]

    for date in date_seq:

        try:
            temp_day = ts.sh_margin_details(start=date,end=date)
            temp_daymid = np.array(temp_day)
            print(date + '---part 1')
            for i in range(len(temp_daymid)):
                resu0 = temp_daymid[i]
                try:
                    sql_insert = "insert into stock_rzrq_detail(state_dt,bd_code,rzye,rqyl)values('%s','%s','%.2f','%.2f')" % (str(date), str(resu0[1]), float(resu0[3]), float(resu0[6]))
                    cursor.execute(sql_insert)
                    db.commit()
                except Exception as exp:
                    print(exp)
                    continue

            temp = ts.sz_margin_details(date)
            resu3_all = np.array(temp)
            print(date + '---part 2')
            for resu3 in resu3_all:
                try:
                    sql_insert3 = "insert into stock_rzrq_detail(state_dt,bd_code,rzye,rqyl)values('%s','%s','%.2f','%.2f')" % (str(date), str(resu3[0]), float(resu3[3]), float(resu3[5]))
                    cursor.execute(sql_insert3)
                    db.commit()
                except Exception as inner_exp:
                    print(inner_exp)
                    continue
        except Exception as exp:
            print(exp)
            continue


    print('ALL Finished!!')



