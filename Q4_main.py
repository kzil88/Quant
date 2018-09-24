import datetime
import tushare as ts
import numpy as np
import Choose
import Filter
import pymysql

if __name__ == '__main__':

    time_temp = datetime.datetime.now() - datetime.timedelta(days=150)
    date_seq_start = time_temp.strftime('%Y-%m-%d')
    # 建立数据库连接,回测时间序列
    db = pymysql.connect(host='127.0.0.1', user='root', passwd='admin', db='stock', charset='utf8')
    cursor = db.cursor()
    sql_done_set = "select prevTradeDate from stock_tradecalall a where a.calendarDate >= '%s' and a.exchangeCD = 'XSHG' and isOpen = 1 order by a.prevTradeDate asc" % (date_seq_start)
    cursor.execute(sql_done_set)
    done_set = cursor.fetchall()
    date_seq = [x[0] for x in done_set]

    for i in range(1,len(date_seq)):
        #ans = Choose.ChooseMain(date_seq[i])
        ans = 6
        single_start_dt = (datetime.datetime.strptime(date_seq[i],'%Y-%m-%d') - datetime.timedelta(days=365)).strftime('%Y-%m-%d')
        single_end_dt = date_seq[i]
        if ans == 1:
            sql_choose = "select * from stock_price_day_list a where a.ro > 1.03 and a.ro < 1.05 and a.close < ma5 and a.close > ma10 and a.close > ma20 and a.close > ma30 and a.close > ma60  and a.state_dt = '%s' order by a.tor*a.vr desc limit 50"%(date_seq[i])
            Filter.filter_main(sql_choose,single_start_dt,single_end_dt,1)
        elif ans == 2:
            sql_choose = "select * from stock_price_day_list a where a.ro > 1.03 and a.ro < 1.05 and a.close < ma5 and a.close < ma10 and a.close > ma20 and a.close > ma30 and a.close > ma60  and a.state_dt = '%s' order by a.tor*a.vr desc limit 50" % (date_seq[i])
            Filter.filter_main(sql_choose, single_start_dt, single_end_dt,2)
        elif ans == 3:
            sql_choose = "select * from stock_price_day_list a where a.ro > 1.03 and a.ro < 1.05 and a.close < ma5 and a.close < ma10 and a.close < ma20 and a.close > ma30 and a.close > ma60  and a.state_dt = '%s' order by a.tor*a.vr desc limit 50" % (date_seq[i])
            Filter.filter_main(sql_choose, single_start_dt, single_end_dt,3)
        elif ans == 4:
            sql_choose = "select * from stock_price_day_list a where a.ro > 1.03 and a.ro < 1.05 and a.close < ma5 and a.close < ma10 and a.close < ma20 and a.close < ma30 and a.close > ma60  and a.state_dt = '%s' order by a.tor*a.vr desc limit 50" % (date_seq[i])
            Filter.filter_main(sql_choose, single_start_dt, single_end_dt,4)
        elif ans == 5:
            sql_choose = "select * from stock_price_day_list a where a.ro > 1.03 and a.ro < 1.05 and a.close < ma5 and a.close < ma10 and a.close < ma20 and a.close < ma30 and a.close < ma60  and a.state_dt = '%s' order by a.tor*a.vr desc limit 50" % (date_seq[i])
            Filter.filter_main(sql_choose, single_start_dt, single_end_dt,5)
        elif ans == 6:
            sql_choose = "select * from good_pool a order by a.f1 desc limit 100"
            Filter.filter_main(sql_choose, single_start_dt, single_end_dt,6)
        print('Runnig to Date :  ' + str(single_end_dt))
    print('ALL FINISHED!!')




