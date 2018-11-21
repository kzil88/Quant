from sklearn import svm
import pymysql.cursors
import datetime
import DC



def goodpool_init(state_dt):
    db = pymysql.connect(host='127.0.0.1', user='root', passwd='admin', db='stock', charset='utf8')
    cursor = db.cursor()
    sql_select = "select distinct stock_code from stock_all a where a.state_dt = '%s'" % (str(state_dt))
    cursor.execute(sql_select)
    done_set = cursor.fetchall()
    sql_select2 = "select distinct stock_code from good_pool a where a.state_dt = '%s'" % (str(state_dt))
    cursor.execute(sql_select2)
    done_set_remove = cursor.fetchall()
    ban_list = [x[0] for x in done_set_remove]
    stock_pool = [x[0] for x in done_set if x[0] not in ban_list]
    model_test_date_start = (datetime.datetime.strptime(state_dt, '%Y-%m-%d') - datetime.timedelta(days=90)).strftime(
        '%Y-%m-%d')
    sql_model_test_date_seq = "select calendarDate from stock_tradecalall a where a.calendarDate >= '%s' and a.calendarDate <= '%s' and a.exchangeCD = 'XSHG' and isOpen = 1 order by a.prevTradeDate asc" % (
    model_test_date_start, state_dt)
    cursor.execute(sql_model_test_date_seq)
    done_set_date_seq = cursor.fetchall()
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
                    datetime.datetime.strptime(model_test_date_seq[d], '%Y-%m-%d') - datetime.timedelta(
                days=365)).strftime(
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
        if return_flag == 1:
            acc = recall = acc_neg = f1 = 0
        else:
            return_flag2 = 0
            for i in range(len(model_test_date_seq) - 4):
                sql_select = "select * from stock_all a where a.stock_code = '%s' and a.state_dt >= '%s' and a.state_dt <= '%s'" % (
                stock, model_test_date_seq[i], model_test_date_seq[i + 4])
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
                sql_update = "update good_pool_model_test w set w.resu_real = '%.2f' where w.state_dt = '%s' and w.stock_code = '%s'" % (
                resu, model_test_date_seq[i], stock)
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
        sql_predict = "select resu_predict from good_pool_model_test a where a.state_dt = '%s'" % (
        model_test_date_seq[-1])
        cursor.execute(sql_predict)
        done_predict = cursor.fetchall()
        predict = 0
        if len(done_predict) != 0:
            predict = int(done_predict[0][0])

        print(str(cnt) + ' of total ' + str(len(stock_pool)) + 'Stock : ' + str(stock) + '   ACC : ' + str(
            acc) + '   RECALL : ' + str(recall) + '   ACC_NEG : ' + str(acc_neg) + '   F1 : ' + str(f1))
        sql_final_insert = "insert into good_pool(state_dt,stock_code,acc,recall,f1,acc_neg,bz,predict)values('%s','%s','%.4f','%.4f','%.4f','%.4f','%s','%s')" % (
        state_dt, stock, acc, recall, f1, acc_neg, 'svm', str(predict))
        cursor.execute(sql_final_insert)
        db.commit()
    print('ALL FINISHED !!')
    db.close()

    return 1

def goodpool_update(stock,state_dt):
    db = pymysql.connect(host='127.0.0.1', user='root', passwd='admin', db='stock', charset='utf8')
    cursor = db.cursor()

    model_test_date_start = (datetime.datetime.strptime(state_dt, '%Y-%m-%d') - datetime.timedelta(days=90)).strftime(
        '%Y-%m-%d')
    sql_model_test_date_seq = "select calendarDate from stock_tradecalall a where a.calendarDate >= '%s' and a.calendarDate <= '%s' and a.exchangeCD = 'XSHG' and isOpen = 1 order by a.prevTradeDate asc" % (
        model_test_date_start, state_dt)
    cursor.execute(sql_model_test_date_seq)
    done_set_date_seq = cursor.fetchall()
    model_test_date_seq = [x[0] for x in done_set_date_seq]

    sql_truncate_model_test = 'truncate table good_pool_model_test'
    cursor.execute(sql_truncate_model_test)
    db.commit()

    return_flag = 0
    for d in range(len(model_test_date_seq)):
        model_test_new_start = (datetime.datetime.strptime(model_test_date_seq[d], '%Y-%m-%d') - datetime.timedelta(days=365)).strftime('%Y-%m-%d')
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

        sql_insert = "insert into good_pool_model_test(state_dt,stock_code,resu_predict)values('%s','%s','%.2f')" % (model_test_new_end, stock, float(ans2[0]))
        cursor.execute(sql_insert)
        db.commit()
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
    sql_predict = "select resu_predict from good_pool_model_test a where a.state_dt = '%s'" % (model_test_date_seq[-1])
    cursor.execute(sql_predict)
    done_predict = cursor.fetchall()
    predict = 0
    if len(done_predict) != 0:
        predict = int(done_predict[0][0])

    sql_final_insert = "insert into good_pool(state_dt,stock_code,acc,recall,f1,acc_neg,bz,predict)values('%s','%s','%.4f','%.4f','%.4f','%.4f','%s','%s')" % (state_dt, stock, acc, recall, f1, acc_neg, 'svm', str(predict))
    cursor.execute(sql_final_insert)
    db.commit()
    db.close()

    return 1
