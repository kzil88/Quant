
import DC
import numpy as np
import datetime
import pymysql
from sklearn import svm
from sklearn import tree
from sklearn import ensemble
import DC_Single



if __name__ == '__main__':


    start_dt = (datetime.datetime.now() - datetime.timedelta(days=366)).strftime('%Y-%m-%d')
    end_dt = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')

    dc = DC_Single.data_collect2('000725',start_dt,end_dt)
    train = dc.data_train
    target = dc.data_target
    test_case = [dc.test_case]
    w = 5*(len(target)/dc.cnt_pos)
    model = svm.SVC(class_weight={1:w})
    model.fit(train,target)
    ans2 = model.predict(test_case)
    print('   ANS : ' + str(ans2[0]) + 'POS_CNT : ' + str(len([x for x in target if x == 1.00])) + '  of total : ' + str(len(target)) + '  start_dt : ' + str(start_dt) + '  end_dt : ' + str(end_dt))
