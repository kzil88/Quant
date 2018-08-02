import datetime
import tushare as ts
import numpy as np
import pymysql
import re

if __name__ == '__main__':


    # 建测试时间序列（筛选出交易日序列）
    ts.set_token('3b7f5162be5869ccae917f5acb3f764c289d5961340737d3645f4516')
    pro = ts.pro_api()

    start_dt = '20100101'
    time_temp = datetime.datetime.now() - datetime.timedelta(days=1)
    end_dt = time_temp.strftime('%Y%m%d')
    end_dt2 = time_temp.strftime('%Y-%m-%d')

    # 取股票代码清单
    resp1 = pro.stock_basic(exchange_id='', is_hs='',list_status='L')
    stock_set = set(resp1.iloc[ : ,0])

    # 建立数据库连接,剔除已入库的部分
    db = pymysql.connect(host='127.0.0.1', user='root', passwd='admin', db='stock', charset='utf8')
    cursor = db.cursor()
    sql_done_set = "SELECT distinct stock_code FROM stock_all where state_dt = '%s'" % (end_dt2)
    try:
        cursor.execute(sql_done_set)
        done_set = cursor.fetchall()
        for row in done_set:
            if row[0] in stock_set:
                stock_set.remove(row[0])
                print('Remove: ' + str(row[0]))
        stock_pool = list(stock_set)
        total = len(stock_pool)
        for i in range(len(stock_pool)):
            # 股价信息
            try:
                df = pro.daily(ts_code=stock_pool[i], start_date=start_dt, end_date=end_dt)
                print('Seq: ' + str(i+1) + ' of ' + str(total) + '   Code: ' + str(stock_pool[i]))
                c_len = df.shape[0]
            except Exception as aa:
                print(aa)
                print('No DATA Code: ' + str(i))
                continue
            for j in range(c_len):
                resu0 = list(df.ix[c_len-1-j])
                resu = []
                for k in range(len(resu0)):
                    if str(resu0[k]) == 'nan':
                        resu.append(-1)
                    else:
                        resu.append(resu0[k])
                state_dt = (datetime.datetime.strptime(resu[1], "%Y%m%d")).strftime('%Y-%m-%d')
                try:
                    sql_insert = "INSERT INTO stock_all(state_dt,stock_code,open,close,high,low,vol,amount,pre_close,amt_change,pct_change) VALUES ('%s', '%s', '%.2f', '%.2f','%.2f','%.2f','%i','%.2f','%.2f','%.2f','%.2f')" % (state_dt,str(resu[0]),float(resu[2]),float(resu[5]),float(resu[3]),float(resu[4]),float(resu[9]),float(resu[10]),float(resu[6]),float(resu[7]),float(resu[8]))
                    cursor.execute(sql_insert)
                    db.commit()
                except Exception as err:
                    print('Inner SQL Errrrrr   ' + str(err))
                    continue

        print('All Finished!')


    except Exception as excp:
        print(str('Errr') + str(i) + str(excp))
    db.close()

