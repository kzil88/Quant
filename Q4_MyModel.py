from sklearn import svm
import pymysql.cursors
import datetime
import DC
import ModelEvaluate

def svm_test(stock,end_dt):
    # 清空内存表
    db = pymysql.connect(host='127.0.0.1', user='root', passwd='admin', db='stock', charset='utf8')
    cursor = db.cursor()
    sql_truncate_model_test = 'truncate table model_test'
    cursor.execute(sql_truncate_model_test)
    db.commit()

    model_test_date_start = (datetime.datetime.strptime(end_dt, '%Y-%m-%d') - datetime.timedelta(days=90)).strftime(
        '%Y-%m-%d')
    sql_model_test_date_seq = "select prevTradeDate from stock_tradecalall a where a.calendarDate >= '%s' and a.calendarDate <= '%s' and a.exchangeCD = 'XSHG' and isOpen = 1 order by a.prevTradeDate asc" % (
    model_test_date_start, end_dt)
    cursor.execute(sql_model_test_date_seq)
    done_set_date_seq = cursor.fetchall()
    model_test_date_seq = [x[0] for x in done_set_date_seq]
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
        # print(end_dt + '   ANS : ' + str(ans2[0]) + 'POS_CNT : ' + str(len([x for x in target if x == 1.00])) + '  of total : ' + str(len(target)) + '  start_dt : ' + str(model_test_new_start) + '  end_dt : ' + str(model_test_new_end))

        sql_insert = "insert into model_test(state_dt,stock_code,resu_predict)values('%s','%s','%.2f')" % (model_test_new_end, stock, float(ans2[0]))
        cursor.execute(sql_insert)
        db.commit()
    if return_flag == 1:
        return 0,0,0,0
    acc, recall, acc_neg, f1 = ModelEvaluate.ModelEvaluate(stock, model_test_date_seq)
    return acc,recall,acc_neg,f1

def svm_predict(stock_code,end_dt):
    model_predict_new_start = (datetime.datetime.strptime(end_dt, '%Y-%m-%d') - datetime.timedelta(days=365)).strftime(
        '%Y-%m-%d')
    dc = DC.data_collect2(stock_code, model_predict_new_start, end_dt)
    train = dc.data_train
    target = dc.data_target
    test_case = [dc.test_case]
    w = 5 * (len(target) / dc.cnt_pos)
    model = svm.SVC(class_weight={1: w})
    model.fit(train, target)
    ans2 = model.predict(test_case)
    return ans2[0]