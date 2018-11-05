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
    sql_select = "SELECT state_dt,stock_code FROM stock_all w where w.big_order_cntro is null and w.big_order_delt is null limit 1000"
    cursor.execute(sql_select)
    done_set = cursor.fetchall()
    batch_cnt = 1
    # 大单数据统计（30万交易额以上）:
    try:
        while len(done_set) > 0:
            for i in range(len(done_set)):
                try:
                    temp_day2 = ts.tick(code=done_set[i][1],date=done_set[i][0],conn=cons)
                    ans = len(temp_day2)
                except Exception as exp:
                    print('Inner Errrr' + str(exp))
                    continue
                df_buy = temp_day2.ix[temp_day2.type == 0]
                cnt_buy = df_buy.ix[df_buy.vol > 400].count()[2]
                vol_buy = df_buy.ix[df_buy.vol > 400].sum()[2]
                if str(vol_buy) == 'nan':
                    vol_buy = 0
                df_sell = temp_day2.ix[temp_day2.type == 1]
                cnt_sell = df_sell.ix[df_sell.vol > 400].count()[2]
                vol_sell = df_sell.ix[df_sell.vol > 400].sum()[2]
                if str(vol_sell) == 'nan':
                    vol_sell = 0
                cntro = 0.00
                if cnt_buy+cnt_sell > 0:
                    cntro = cnt_buy/(cnt_buy+cnt_sell)
                delt = vol_buy-vol_sell

                print('Batch_Seq : ' + str(batch_cnt) + '   ' + str(i+1) + ' of '+ str(len(done_set)) + '   stock_code : ' +str(done_set[i][1]) + '  Date : ' + str(done_set[i][0]))
                sql_update = "update stock_all w set w.big_order_cntro = '%.2f' where w.state_dt = '%s' and w.stock_code = '%s'"%(cntro,done_set[i][0],done_set[i][1])
                cursor.execute(sql_update)
                db.commit()
                sql_update2 = "update stock_all w set w.big_order_delt = '%i' where w.state_dt = '%s' and w.stock_code = '%s'"%(delt,done_set[i][0],done_set[i][1])
                cursor.execute(sql_update2)
                db.commit()
            cursor.execute(sql_select)
            done_set = cursor.fetchall()
            batch_cnt += 1

    except Exception as excp:
        # db.rollback()
        print(str('Errr') + str(i) + str(excp))
    db.close()
    print('ALL Finished!!')



