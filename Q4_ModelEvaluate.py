import datetime
import tushare as ts
import numpy as np
import pymysql

def ModelEvaluate(stock,date_seq):

    db = pymysql.connect(host='127.0.0.1', user='root', passwd='admin', db='stock', charset='utf8')
    cursor = db.cursor()

    for i in range(len(date_seq)-4):
        sql_select = "select * from stock_price_day_list a where a.stock_code = '%s' and a.state_dt >= '%s' and a.state_dt <= '%s'"%(stock,date_seq[i],date_seq[i+4])
        cursor.execute(sql_select)
        done_set2 = cursor.fetchall()
        close_list = [x[3] for x in done_set2]
        if len(close_list) <= 1:
            return 0,0,0,0
        after_max = max(close_list[1:len(close_list)])
        after_min = min(close_list[1:len(close_list)])
        resu = 0
        if after_max/close_list[0] >= 1.03:
            resu = 1
        elif  after_min/close_list[0] < 0.97:
            resu = -1
        sql_update = "update model_test w set w.resu_real = '%.2f' where w.state_dt = '%s' and w.stock_code = '%s'"%(resu,date_seq[i],stock)
        cursor.execute(sql_update)
        db.commit()

    sql_resu_acc_son = "select count(*) from model_test a where a.resu_real is not null and a.resu_predict = 1 and a.resu_real = 1"
    cursor.execute(sql_resu_acc_son)
    acc_son = cursor.fetchall()[0][0]
    sql_resu_acc_mon = "select count(*) from model_test a where a.resu_real is not null and a.resu_real = 1"
    cursor.execute(sql_resu_acc_mon)

    acc_mon = cursor.fetchall()[0][0]
    if acc_mon == 0:
        return 0,0,0,0
    acc = acc_son/acc_mon

    sql_resu_recall_son = "select count(*) from model_test a where a.resu_real is not null and a.resu_predict = a.resu_real"
    cursor.execute(sql_resu_recall_son)
    recall_son = cursor.fetchall()[0][0]
    sql_resu_recall_mon = "select count(*) from model_test a where a.resu_real is not null"
    cursor.execute(sql_resu_recall_mon)
    recall_mon = cursor.fetchall()[0][0]
    recall = recall_son / recall_mon

    sql_resu_acc_neg_son = "select count(*) from model_test a where a.resu_real is not null and a.resu_predict = -1 and a.resu_real = -1"
    cursor.execute(sql_resu_acc_neg_son)
    acc_neg_son = cursor.fetchall()[0][0]
    sql_resu_acc_neg_mon = "select count(*) from model_test a where a.resu_real is not null and a.resu_real = -1"
    cursor.execute(sql_resu_acc_neg_mon)
    acc_neg_mon = cursor.fetchall()[0][0]
    if acc_neg_mon == 0:
        acc_neg_mon = -1
    acc_neg = acc_neg_son / acc_neg_mon
    if acc+recall == 0:
        return 0,0,0,0
    f1 = (2*acc*recall)/(acc+recall)

    print(date_seq[-1] + '   ACC : ' + str(acc) + '   RECALL : ' + str(recall)+ '   ACC_NEG : ' + str(acc_neg) + '   F1 : ' + str(f1))

    return acc,recall,acc_neg,f1

