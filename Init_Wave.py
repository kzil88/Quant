import datetime
import tushare as ts
import numpy as np
import pymysql
import re

def update_bigorder(stock_pool,date_seq):
    cons = ts.get_apis()
    # 建立数据库连接,剔除已入库的部分
    db = pymysql.connect(host='127.0.0.1', user='root', passwd='admin', db='stock', charset='utf8')
    cursor = db.cursor()
    for s in range(len(stock_pool)):
        for d in range(len(date_seq)):
            try:
                temp_day2 = ts.tick(code=stock_pool[s], date=date_seq[d], conn=cons)
                print(temp_day2)
                ans = len(temp_day2)
            except Exception as exp:
                print('Inner Errrr' + str(exp))
    #             continue
    #                 df_buy = temp_day2.ix[temp_day2.type == 0]
    #                 #cnt_buy = df_buy.ix[df_buy.vol > 400].count()[2]
    #                 #vol_buy = df_buy.ix[df_buy.vol > 400].sum()[2]
    #                 #if cnt_buy == 0:
    #                 cnt_buy = df_buy.ix[df_buy.vol * df_buy.price * 100 > 1000000].count()[2]
    #                 vol_buy = df_buy.ix[df_buy.vol * df_buy.price * 100 > 1000000].sum()[2]
    #                 if str(vol_buy) == 'nan':
    #                     vol_buy = 0
    #                 df_sell = temp_day2.ix[temp_day2.type == 1]
    #                 #cnt_sell = df_sell.ix[df_sell.vol > 400].count()[2]
    #                 #vol_sell = df_sell.ix[df_sell.vol > 400].sum()[2]
    #                 #if cnt_sell == 0:
    #                 cnt_sell = df_sell.ix[df_sell.vol * df_sell.price * 100 > 1000000].count()[2]
    #                 vol_sell = df_sell.ix[df_sell.vol * df_sell.price * 100 > 1000000].sum()[2]
    #                 if str(vol_sell) == 'nan':
    #                     vol_sell = 0
    #                 cntro = 0.00
    #                 if cnt_buy + cnt_sell > 0:
    #                     cntro = cnt_buy / (cnt_buy + cnt_sell)
    #                 delt = vol_buy - vol_sell
    #
    #                 print('Stock_Seq : ' + str(s+1) + '   Inner_Seq : ' + str(i + 1) + ' of ' + str(
    #                     len(done_set)) + '   stock_code : ' + str(done_set[i][1]) + '  Date : ' + str(done_set[i][0]))
    #                 sql_update = "update stock_all w set w.big_order_cntro = '%.2f' where w.state_dt = '%s' and w.stock_code = '%s'" % (
    #                 cntro, done_set[i][0], done_set[i][1])
    #                 cursor.execute(sql_update)
    #                 db.commit()
    #                 sql_update2 = "update stock_all w set w.big_order_delt = '%i' where w.state_dt = '%s' and w.stock_code = '%s'" % (
    #                 delt, done_set[i][0], done_set[i][1])
    #                 cursor.execute(sql_update2)
    #                 db.commit()
    #         sql_resu = "select state_dt,big_order_cntro,big_order_delt from stock_all a where a.stock_code = '%s' order by state_dt desc limit 5"%(stock_pool[s])
    #         cursor.execute(sql_resu)
    #         done_set_resu = cursor.fetchall()
    #         db.commit()
    #         print('Stock : ' + str(stock_pool[s]))
    #         print('Date_Seq : ',end='')
    #         print([str(x[0]) for x in done_set_resu][::-1])
    #         print('Big_Order_Cntro : ', end='')
    #         print([str(x[1]) for x in done_set_resu][::-1])
    #         print('Big_Order_Delt : ', end='')
    #         print([str(x[2]) for x in done_set_resu][::-1])
    #         del done_set_resu
    #         del done_set
    #
    #     except Exception as excp:
    #         # db.rollback()
    #         print(str('Errr') + str(i) + str(excp))
    # db.close()
    # print('ALL Finished!!')
    #


if __name__ == '__main__':
    stock_pool = ['002008']
    update_bigorder(stock_pool,['2018-04-04'])




