from sklearn import svm
import pymysql.cursors
import datetime
import DC
import tushare as ts
import re


def Init_stock_all(code_set,end_dt):

    cons = ts.get_apis()

    # 建立数据库连接,剔除已入库的部分
    db = pymysql.connect(host='127.0.0.1', user='root', passwd='admin', db='stock', charset='utf8')
    cursor = db.cursor()
    sql_done_set = "SELECT distinct stock_code FROM stock_all where state_dt = '%s'" % (end_dt)
    #sql_done_set = "SELECT distinct stock_code FROM stock_price_day_list"
    cursor.execute(sql_done_set)
    done_set = cursor.fetchall()
    remove_set = [x[0] for x in done_set]
    stock_pool = [x for x in code_set if x not in remove_set]
    cnt = 1
    total = len(stock_pool)
    for i in stock_pool:
        try:
            temp_day = ts.bar(code=i, conn = cons,freq='D',start_date='2016-01-01',end_date=end_dt,ma=[5,10,20,30,60],factors=['vr','tor'],adj='qfq')
            print('Seq: ' + str(cnt) + ' of ' + str(total) + '  Code: ' + str(i))
            c_len = temp_day.shape[0]
        except Exception as aa:
            print('No DATA Code: ' + str(i))
            continue
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
                sql_insert = "INSERT INTO stock_all(state_dt,stock_code,open,close,high,low,vol,amount,tor,vr,ma5,ma10,ma20,ma30,ma60,ro) VALUES ('%s', '%s', '%.2f', '%.2f','%.2f','%.2f','%i','%.2f','%.2f','%.2f','%.2f','%.2f','%.2f','%.2f','%.2f','%.4f')" % (state_dt,resu[0],resu[1],resu[2],resu[3],resu[4],resu[5],resu[6],resu[7],resu[8],resu[9],resu[10],resu[11],resu[12],resu[13],ro)
                cursor.execute(sql_insert)
                db.commit()
            except Exception as err:
                continue
            cnt += 1

    print('Init Stock_All Finished!')
    cursor.close()
    db.close()
    return 1

if __name__ == '__main__':

    #设定更新日期为上一个交易日
    time_temp = datetime.datetime.now() - datetime.timedelta(days=3)
    end_dt = time_temp.strftime('%Y-%m-%d')
    sql_before = "select a.prevTradeDate from stock_tradecalall a where a.calendarDate = '%s' and a.isOpen = 1 and a.exchangeCD = 'XSHG'"%(end_dt)
    db = pymysql.connect(host='127.0.0.1', user='root', passwd='admin', db='stock', charset='utf8')
    cursor = db.cursor()
    cursor.execute(sql_before)
    done_beofore = cursor.fetchall()
    db.commit()
    end_dt_before = done_beofore[0][0]
    #取上上个交易日F1前30的股票
    sql_select = "select distinct stock_code from good_pool_all a where a.state_dt = '%s' order by f1 desc limit 30"%(str(end_dt_before))
    cursor.execute(sql_select)
    done_set = cursor.fetchall()
    db.commit()
    #取上一个交易日已运算的股票用于去重，断点重算
    sql_select2 = "select distinct stock_code from good_pool_all a where a.state_dt = '%s'"%(str(end_dt))
    cursor.execute(sql_select2)
    done_set_remove = cursor.fetchall()
    db.commit()
    ban_list = [x[0] for x in done_set_remove]
    stock_pool = [x[0] for x in done_set if x[0] not in ban_list]

    #更新待运算股票在上一个交易日的股价信息
    ans_loc = Init_stock_all(set(stock_pool),end_dt)

    #建回测时间序列，建模运算
    model_test_date_start = (datetime.datetime.strptime(end_dt, '%Y-%m-%d') - datetime.timedelta(days=90)).strftime('%Y-%m-%d')
    sql_model_test_date_seq = "select calendarDate from stock_tradecalall a where a.calendarDate >= '%s' and a.calendarDate <= '%s' and a.exchangeCD = 'XSHG' and isOpen = 1 order by a.calendarDate asc" % (model_test_date_start, end_dt)
    cursor.execute(sql_model_test_date_seq)
    done_set_date_seq = cursor.fetchall()
    db.commit()
    model_test_date_seq = [x[0] for x in done_set_date_seq]

    cnt = 0
    for stock in stock_pool:
        cnt += 1
        sql_truncate_model_test = 'truncate table good_pool_model_test'
        cursor.execute(sql_truncate_model_test)
        db.commit()

        return_flag = 0
        for d in range(len(model_test_date_seq)):
            model_test_new_start = (
                datetime.datetime.strptime(model_test_date_seq[d], '%Y-%m-%d') - datetime.timedelta(days=365)).strftime(
                '%Y-%m-%d')
            model_test_new_end = model_test_date_seq[d]
            try:
                dc = DC.data_collect2(stock, model_test_new_start, model_test_new_end)
            except Exception as exp:
                print("DC Errrrr")
                return_flag = 1
                break
            train = dc.data_train
            target = dc.data_target
            test_case = [dc.test_case]
            if dc.cnt_pos == 0:
                return_flag = 1
                break
            w = 5 * (len(target) / dc.cnt_pos)
            if len(target) / dc.cnt_pos == 1:
                return_flag = 1
                break
            model = svm.SVC(class_weight={1: w})
            model.fit(train, target)
            ans2 = model.predict(test_case)

            sql_insert = "insert into good_pool_model_test(state_dt,stock_code,resu_predict)values('%s','%s','%.2f')" % (
            model_test_new_end, stock, float(ans2[0]))
            cursor.execute(sql_insert)
            db.commit()
            del model
            del ans2
            del train
            del target
            del test_case
        if return_flag == 1:
            acc = recall = acc_neg = f1 = 0
        else:
            return_flag2 = 0
            for i in range(len(model_test_date_seq) - 4):
                sql_select = "select * from stock_all a where a.stock_code = '%s' and a.state_dt >= '%s' and a.state_dt <= '%s'" % (stock, model_test_date_seq[i], model_test_date_seq[i + 4])
                cursor.execute(sql_select)
                done_set2 = cursor.fetchall()
                close_list = [x[3] for x in done_set2]
                if len(close_list) <= 1:
                    return_flag2 = 1
                    break
                after_max = max(close_list[1:len(close_list)])
                after_min = min(close_list[1:len(close_list)])
                resu = 0
                if after_max / close_list[0] >= 1.03:
                    resu = 1
                elif after_min / close_list[0] < 0.97:
                    resu = -1
                sql_update = "update good_pool_model_test w set w.resu_real = '%.2f' where w.state_dt = '%s' and w.stock_code = '%s'" % (resu, model_test_date_seq[i], stock)
                cursor.execute(sql_update)
                db.commit()
            if return_flag2 == 1:
                acc = recall = acc_neg = f1 = 0
            else:
                sql_resu_acc_son = "select count(*) from good_pool_model_test a where a.resu_real is not null and a.resu_predict = 1 and a.resu_real = 1"
                cursor.execute(sql_resu_acc_son)
                acc_son = cursor.fetchall()[0][0]
                sql_resu_acc_mon = "select count(*) from good_pool_model_test a where a.resu_real is not null and a.resu_real = 1"
                cursor.execute(sql_resu_acc_mon)
                acc_mon = cursor.fetchall()[0][0]
                if acc_mon == 0:
                    acc = recall = acc_neg = f1 = 0
                else:
                    acc = acc_son / acc_mon

                sql_resu_recall_son = "select count(*) from good_pool_model_test a where a.resu_real is not null and a.resu_predict = a.resu_real"
                cursor.execute(sql_resu_recall_son)
                recall_son = cursor.fetchall()[0][0]
                sql_resu_recall_mon = "select count(*) from good_pool_model_test a where a.resu_real is not null"
                cursor.execute(sql_resu_recall_mon)
                recall_mon = cursor.fetchall()[0][0]
                recall = recall_son / recall_mon

                sql_resu_acc_neg_son = "select count(*) from good_pool_model_test a where a.resu_real is not null and a.resu_predict = -1 and a.resu_real = -1"
                cursor.execute(sql_resu_acc_neg_son)
                acc_neg_son = cursor.fetchall()[0][0]
                sql_resu_acc_neg_mon = "select count(*) from good_pool_model_test a where a.resu_real is not null and a.resu_real = -1"
                cursor.execute(sql_resu_acc_neg_mon)
                acc_neg_mon = cursor.fetchall()[0][0]
                if acc_neg_mon == 0:
                    acc_neg_mon = -1
                else:
                    acc_neg = acc_neg_son / acc_neg_mon
                if acc + recall == 0:
                    acc = recall = acc_neg = f1 = 0
                else:
                    f1 = (2 * acc * recall) / (acc + recall)


        sql_predict = "select resu_predict from good_pool_model_test a where a.state_dt = '%s'"%(model_test_date_seq[-1])
        cursor.execute(sql_predict)
        done_predict = cursor.fetchall()

        # dc_new_start = (datetime.datetime.strptime(model_test_date_seq[-1], '%Y-%m-%d') - datetime.timedelta(days=365)).strftime('%Y-%m-%d')
        # model_test_new_end = model_test_date_seq[d]
        # new_dc = DC.data_collect2(stock, dc_new_start, model_test_date_seq[-1])
        # new_train = new_dc.data_train
        # new_target = new_dc.data_target
        # new_test_case = [new_dc.test_case]
        #
        # w = 5 * (len(new_target) / new_dc.cnt_pos)
        #
        # new_model = svm.SVC(class_weight={1: w})
        # # new_model = svm.SVC(class_weight={1: w},probability=True,verbose=True,shrinking=True)
        # new_model.fit(new_train, new_target)
        # # ans_fin = new_model.predict_proba(new_test_case)
        # ans_fin2 = new_model.predict(new_test_case)

        predict = 0
        if len(done_predict) != 0:
            predict = int(done_predict[0][0])

        print(str(cnt) + ' of total ' + str(len(stock_pool))+ '  Stock : ' + str(stock) + '   ACC : ' + str(acc) + '   RECALL : ' + str(recall) + '   ACC_NEG : ' + str(acc_neg) + '   F1 : ' + str(f1) + '   Predict : ' + str(predict))
        sql_final_insert = "insert into good_pool_all(state_dt,stock_code,acc,recall,f1,acc_neg,bz,predict)values('%s','%s','%.4f','%.4f','%.4f','%.4f','%s','%s')" % (end_dt, stock, acc,recall,f1,acc_neg,'svm',str(predict))
        cursor.execute(sql_final_insert)
        db.commit()
    print('ALL FINISHED !!')
    db.close()




