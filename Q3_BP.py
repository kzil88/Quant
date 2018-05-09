from keras.models import Sequential
from keras.layers import Dense, Dropout, Activation
from keras.optimizers import SGD
import DC
from keras.layers import Flatten
import numpy as np
import keras
import datetime
import pymysql


if __name__ == '__main__':
    time_temp = datetime.datetime.now() - datetime.timedelta(days=90)
    date_seq_start = time_temp.strftime('%Y-%m-%d')
    end_dt = (datetime.datetime.now() -datetime.timedelta(days=1)) .strftime('%Y-%m-%d')
    # 建立数据库连接,回测时间序列


    dc = DC.data_collect2('000725',date_seq_start,end_dt)
    score_list = []
    resu_list = []
    train = dc.data_train
    target = dc.data_target


    model = Sequential()
    model.add(Dense(64, activation='linear', input_dim=14))
    model.add(Dropout(0.5))
    model.add(Dense(64, activation='sigmoid'))
    model.add(Dropout(0.5))
    model.add(Dense(1, activation='relu'))
    sgd = SGD(lr=0.01, decay=1e-6, momentum=0.9, nesterov=True)
    model.compile(loss='logcosh', optimizer=sgd, metrics=['accuracy'])


    for i in range(5):
        model.fit(train, target, epochs=2000)
        score = model.evaluate(train, target, batch_size=128)
        print('SCORE:' + str(score[0]))
        test_case = np.array([dc.test_case])
        ans2 = model.predict(test_case)
        resu_list.append(ans2[0][0])
        score_list.append(score)
        print('RESU  '+str(i+1)+'  :  '+str(ans2[0][0]))
        dc.refreshDATA(ans2[0][0])
        train = dc.data_train
        target = dc.data_target

    print(score_list)
    print(resu_list)
    print(date_seq_start)
    print(end_dt)