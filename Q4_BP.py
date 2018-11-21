from keras.models import Sequential
from keras.layers import Dense, Dropout, Activation
from keras.optimizers import SGD
import DC
from keras.layers import Flatten
import numpy as np
import keras
import datetime
import pymysql
from sklearn import svm
from sklearn import tree
from sklearn import ensemble



if __name__ == '__main__':

    
    time_temp = datetime.datetime.now() - datetime.timedelta(days=90)
    date_seq_start = time_temp.strftime('%Y-%m-%d')
    # 建立数据库连接,回测时间序列
    db = pymysql.connect(host='127.0.0.1', user='root', passwd='admin', db='stock', charset='utf8')
    cursor = db.cursor()
    sql_done_set = "select prevTradeDate from stock_tradecalall a where a.calendarDate >= '%s' and a.exchangeCD = 'XSHG' and isOpen = 1 order by a.prevTradeDate asc" % (date_seq_start)
    cursor.execute(sql_done_set)
    done_set = cursor.fetchall()
    date_seq = [x[0] for x in done_set]

    for i in range(len(date_seq)):
        start_dt = (datetime.datetime.strptime(date_seq[i],'%Y-%m-%d') - datetime.timedelta(days=90)).strftime('%Y-%m-%d')
        end_dt = date_seq[i]
        dc = DC.data_collect2('002008',start_dt,end_dt)
        train = dc.data_train
        target = dc.data_target
        test_case = np.array([dc.test_case])

        #model = ensemble.RandomForestClassifier(n_estimators= 10)
        model = svm.SVC(class_weight={1:10000000})
        model.fit(train,target)
        ans2 = model.predict(test_case)
        print(date_seq[i] + '   ANS : ' + str(ans2[0]))


        #target = to_categorical(target, num_classes=None)

        #########################################
        # MLP 模型
        # model = Sequential()
        # model.add(Dense(64, activation='linear', input_dim=14))
        # model.add(Dropout(0.5))
        # model.add(Dense(64, activation='relu'))
        # model.add(Dropout(0.5))
        # model.add(Dense(2, activation='softmax'))
        # sgd = SGD(lr=0.01, decay=1e-6, momentum=0.9, nesterov=True)
        # model.compile(loss='categorical_crossentropy', optimizer=sgd, metrics=['categorical_accuracy'])
        # model.fit(train, target, epochs=200,shuffle=False)
        #
        # score = model.evaluate(train, target, batch_size=128)
        # print('SCORE:' + str(score))
        # print(model.metrics_names)
        # test_case = np.array([dc.test_case])
        # ans2 = model.predict(test_case)
        # print('RESU  :  '+str(ans2))
        #sql_insert = "insert into model_test2(state_dt,stock_code,resu_predict)values('%s','%s','%.2f')" % (date_seq[i],'000725',float(ans2[0][0]))
        #key = 0
        #if ans2[0][0] >= ans2[0][1]:
        #    key = 1
        #sql_insert = "insert into model_test2(state_dt,stock_code,resu_predict)values('%s','%s','%.2f')" % (date_seq[i],'000725',float(key))
        sql_insert = "insert into model_test2(state_dt,stock_code,resu_predict)values('%s','%s','%.2f')" % (date_seq[i], '002008', float(ans2[0]))
        cursor.execute(sql_insert)
        db.commit()
    print('ALL Finished!!')
 #[[ 0.6396479  0.3603521]]  [[ 0.57934844  0.42065158]]
