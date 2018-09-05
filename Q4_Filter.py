import pymysql.cursors
import DC
#from keras.models import Sequential
#from keras.layers import Dense, Dropout, Activation
#from keras.optimizers import SGD
import numpy as np
import Deal
import tushare as ts
import Choose
from sklearn import svm
import datetime
import ModelEvaluate
import Operator
import MyModel

def filter_main(sql_choose,start_dt,end_dt,slayer):
    # 建立数据库连接
    db = pymysql.connect(host='127.0.0.1', user='root', passwd='admin', db='stock', charset='utf8')
    cursor = db.cursor()

    #先更新持股天数
    sql_update_hold_days = 'update my_stock_pool w set w.hold_days = w.hold_days + 1'
    cursor.execute(sql_update_hold_days)
    db.commit()

    #先卖出
    deal = Deal.Deal()
    stock_pool_local = deal.stock_pool
    for stock in stock_pool_local:
        ans = Operator.sell(stock,end_dt)

    #后买入
    deal_buy = Deal.Deal()
    if deal_buy.cur_money_rest > 20000:
        sql_ban_pool = "select distinct stock_code from ban_list"
        cursor.execute(sql_ban_pool)
        done_ban_pool = cursor.fetchall()
        ban_list = [x[0] for x in done_ban_pool]

        sql_first_pool = "select distinct stock_code from good_pool order by f1 desc"
        cursor.execute(sql_first_pool)
        done_good_pool = cursor.fetchall()
        good_pool = [x[0] for x in done_good_pool if x[0] not in ban_list]
        flag = 0
        for i in range(len(good_pool)):
            acc, recall, acc_neg, f1 = MyModel.svm_test(good_pool[i], end_dt)
            if acc >= 0.6 and f1 >= 0.5:
                sql_insert_good_pool = "insert into good_pool(state_dt,stock_code,acc,recall,f1,acc_neg)values('%s','%s','%.4f','%.4f','%.4f','%.4f')" % (end_dt, good_pool[i], acc, recall, f1, acc_neg)
                cursor.execute(sql_insert_good_pool)
                db.commit()
                ans = MyModel.svm_predict(good_pool[i], end_dt)
                if ans == 1.00:
                    ans = Operator.buy(good_pool[i], end_dt, 20000, slayer)
                    flag = 1
                    break
        deal_buy = Deal.Deal()
        if flag == 0 and deal_buy.cur_money_rest >= 20000:
            cursor.execute(sql_choose)
            done_set2 = cursor.fetchall()
            if len(done_set2) > 0:
                temp_stock_pool_buy = [x[1] for x in done_set2 if x[1] not in good_pool]
                stock_pool_buy = [x for x in temp_stock_pool_buy if x not in ban_list]
                for stock_buy in stock_pool_buy:

                    acc, recall, acc_neg, f1 = MyModel.svm_test(stock_buy, end_dt)
                    if acc >= 0.6 and f1 >=0.5 :
                        sql_insert_good_pool = "insert into good_pool(state_dt,stock_code,acc,recall,f1,acc_neg)values('%s','%s','%.4f','%.4f','%.4f','%.4f')" % (end_dt,stock_buy,acc,recall,f1,acc_neg)
                        cursor.execute(sql_insert_good_pool)
                        db.commit()
                        ans = MyModel.svm_predict(stock_buy, end_dt)
                        if ans == 1.00:
                            ans = Operator.buy(stock_buy,end_dt,20000,slayer)
    db.close()
