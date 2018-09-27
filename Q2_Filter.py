import pymysql.cursors
import DC
from keras.models import Sequential
from keras.layers import Dense, Dropout, Activation
from keras.optimizers import SGD
import numpy as np
import Deal
import tushare as ts
import Choose


def filter_main(start_dt,end_dt):
    # 建立数据库连接
    db = pymysql.connect(host='127.0.0.1', user='root', passwd='admin', db='stock', charset='utf8')
    cursor = db.cursor()
    #先更新持股天数
    sql_update_hold_days = 'update my_stock_pool w set w.hold_days = w.hold_days + 1'
    cursor.execute(sql_update_hold_days)
    db.commit()


    #先卖出
    deal = Deal.Deal()
    for stock in deal.stock_pool:
        init_price = deal.stock_map1[stock]
        hold_vol = deal.stock_map2[stock]
        hold_days = deal.stock_map3[stock]
        sell_price_temp = ts.get_k_data(code=stock,start=start_dt,end=end_dt)
        sell_price_list = np.array(sell_price_temp.iloc[0:, [2]].astype('float32'), dtype=np.float).ravel()
        sell_price = sell_price_list[-1]

        if sell_price > init_price*1.03 and hold_vol > 0:
            new_money_lock = deal.cur_money_lock - init_price*hold_vol
            new_money_rest = deal.cur_money_rest + sell_price*hold_vol
            new_capital = deal.cur_capital + (sell_price-init_price)*hold_vol
            new_profit = (sell_price-init_price)*hold_vol
            new_profit_rate = sell_price/init_price
            sql_sell_insert = "insert into my_capital(capital,money_lock,money_rest,deal_action,stock_code,stock_vol,profit,profit_rate,bz,state_dt,deal_price)values('%f','%f','%f','%s','%s','%f','%f','%f','%s','%s','%f')" %(new_capital,new_money_lock,new_money_rest,'SELL',stock,hold_vol,new_profit,new_profit_rate,'GOODSELL',end_dt,sell_price)
            cursor.execute(sql_sell_insert)
            db.commit()
            sql_sell_update = "delete from my_stock_pool where stock_code = '%s'" % (stock)
            cursor.execute(sql_sell_update)
            db.commit()
            continue

        if sell_price < init_price*0.95 and hold_vol > 0:
            new_money_lock = deal.cur_money_lock - init_price*hold_vol
            new_money_rest = deal.cur_money_rest + sell_price*hold_vol
            new_capital = deal.cur_capital + (sell_price-init_price)*hold_vol
            new_profit = (sell_price-init_price)*hold_vol
            new_profit_rate = sell_price/init_price
            sql_sell_insert2 = "insert into my_capital(capital,money_lock,money_rest,deal_action,stock_code,stock_vol,profit,profit_rate,bz,state_dt,deal_price)values('%f','%f','%f','%s','%s','%f','%f','%f','%s','%s','%f')" %(new_capital,new_money_lock,new_money_rest,'SELL',stock,hold_vol,new_profit,new_profit_rate,'BADSELL',end_dt,sell_price)
            cursor.execute(sql_sell_insert2)
            db.commit()
            sql_sell_update2 = "delete from my_stock_pool where stock_code = '%s'" % (stock)
            cursor.execute(sql_sell_update2)
            db.commit()
            sql_ban_insert = "insert into ban_list(stock_code) values ('%s')" %(stock)
            cursor.execute(sql_ban_insert)
            db.commit()
            continue

        if hold_days >= 5 and hold_vol > 0:
            new_money_lock = deal.cur_money_lock - init_price * hold_vol
            new_money_rest = deal.cur_money_rest + sell_price * hold_vol
            new_capital = deal.cur_capital + (sell_price - init_price) * hold_vol
            new_profit = (sell_price - init_price) * hold_vol
            new_profit_rate = sell_price / init_price
            sql_sell_insert3 = "insert into my_capital(capital,money_lock,money_rest,deal_action,stock_code,stock_vol,profit,profit_rate,bz,state_dt,deal_price)values('%f','%f','%f','%s','%s','%f','%f','%f','%s','%s','%f')" % (new_capital, new_money_lock, new_money_rest, 'OVERTIME', stock, hold_vol, new_profit, new_profit_rate,'OVERTIMESELL', end_dt,sell_price)
            cursor.execute(sql_sell_insert3)
            db.commit()
            sql_sell_update3 = "delete from my_stock_pool where stock_code = '%s'" % (stock)
            cursor.execute(sql_sell_update3)
            db.commit()
            continue


    deal_buy = Deal.Deal()
    #后买入
    if deal_buy.cur_money_rest > 20000:
        #Choose.choose_main(start_dt, end_dt)
        try:
            resu_list = []
            score_list = []
            #sql_select = "select a.stock_code from q1_cs a where a.calc_date ='%s' order by af/freq desc limit 10" %(end_dt)
            sql_select = "select a.stock_code from q1_cs a order by af/freq desc limit 50"
            cursor.execute(sql_select)
            done_set2 = cursor.fetchall()
            for row in done_set2:
                if row[0] in deal_buy.ban_list:
                    continue
                dc = DC.data_collect2(row[0],start_dt,end_dt)
                if (dc.cnt_good_buy[-1]*dc.cnt_good_sell[-1])/min(1,(dc.cnt_bad_sell[-1]+dc.cnt_risk[-1])) < 1:
                    continue
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
                model.fit(train, target, epochs=1000)
                score0 = model.evaluate(train, target, batch_size=128)
                if float(score0[0]) > 0.5 or str(score0[0]) == 'nan':
                    continue
                for i in range(5):
                    model.fit(train, target, epochs=5000)
                    score = model.evaluate(train, target, batch_size=128)
                    print('SCORE:' + str(score[0]))
                    test_case = np.array([dc.test_case])
                    ans2 = model.predict(test_case)
                    resu_list.append(ans2[0][0])
                    score_list.append(score)
                    print('RESU  ' + str(i + 1) + '  :  ' + str(ans2[0][0]))
                    dc.refreshDATA(ans2[0][0])
                    train = dc.data_train
                    target = dc.data_target
                print(score_list)
                print(resu_list)

                if max(resu_list) > dc.avg[-6]*1.05 and (sum(resu_list)/5) > dc.avg[-6]*1.02:
                    vol,rest = divmod(min(deal_buy.cur_money_rest,30000),dc.list_close[-6]*100)
                    vol = vol*100
                    sql_buy_update2  = "insert into my_capital(capital,money_lock,money_rest,deal_action,stock_code,stock_vol,state_dt,deal_price)VALUES ('%f', '%f', '%f','%s','%s','%i','%s','%f')" % (deal_buy.cur_capital-vol*dc.list_close[-6]*0.0005,deal_buy.cur_money_lock+vol*dc.list_close[-6],deal_buy.cur_money_rest-vol*dc.list_close[-6]*1.0005,'buy',row[0],vol,end_dt,dc.list_close[-6])
                    cursor.execute(sql_buy_update2)
                    db.commit()
                    if row[0] in deal_buy.stock_all:
                        new_buy_price = (deal_buy.stock_map1[row[0]]*deal_buy.stock_map2[row[0]] + vol*dc.list_close[-6])/(deal_buy.stock_map2[row[0]] + vol)
                        new_vol = deal_buy.stock_map2[row[0]] + vol
                        sql_buy_update3 = "update my_stock_pool w set w.buy_price = (select '%f' from dual) where w.stock_code = '%s'" % (new_buy_price,row[0])
                        sql_buy_update3b = "update my_stock_pool w set w.hold_vol = (select '%i' from dual) where w.stock_code = '%s'" % (new_vol, row[0])
                        sql_buy_update3c = "update my_stock_pool w set w.hold_days = (select '%i' from dual) where w.stock_code = '%s'" % (1, row[0])
                        cursor.execute(sql_buy_update3)
                        cursor.execute(sql_buy_update3b)
                        cursor.execute(sql_buy_update3c)
                        db.commit()
                    else:
                        sql_buy_update3 = "insert into my_stock_pool(stock_code,buy_price,hold_vol,hold_days) VALUES ('%s','%f','%i','%i')" %(row[0],dc.list_close[-6],vol,int(1))
                        cursor.execute(sql_buy_update3)
                        db.commit()
                    break

        except Exception as excp:
            #db.rollback()
            print(excp)
    db.close()
