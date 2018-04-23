from keras.models import Sequential
from keras.layers import Dense, Dropout, Activation
from keras.optimizers import SGD
import DC
from keras.layers import Flatten
import numpy as np
import keras


if __name__ == '__main__':
    dc = DC.DC('600519')
    score_list = []
    resu_list = []
    train = dc.data_train
    target = dc.data_target


    model = Sequential()
    model.add(Dense(64, activation='linear', input_dim=13))
    model.add(Dropout(0.5))
    model.add(Dense(64, activation='sigmoid'))
    model.add(Dropout(0.5))
    model.add(Dense(1, activation='relu'))
    sgd = SGD(lr=0.01, decay=1e-6, momentum=0.9, nesterov=True)
    model.compile(loss='logcosh', optimizer=sgd, metrics=['accuracy'])


    for i in range(5):
        model.fit(train, target, epochs=20000)
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
