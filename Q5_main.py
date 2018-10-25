import datetime
import pymysql
import GoodPool as gp
import Filter

if __name__ == '__main__':
    # 建立数据库连接,回测时间序列
    #time_temp = datetime.datetime.now() - datetime.timedelta(days=333)
    #date_seq_start = time_temp.strftime('%Y-%m-%d')
    date_seq_start = '2017-04-09'
    db = pymysql.connect(host='127.0.0.1', user='root', passwd='admin', db='stock', charset='utf8')
    cursor = db.cursor()
    sql_done_set = "select calendarDate from stock_tradecalall a where a.calendarDate >= '%s' and a.exchangeCD = 'XSHG' and isOpen = 1 order by a.prevTradeDate asc" % (date_seq_start)
    cursor.execute(sql_done_set)
    done_set = cursor.fetchall()
    db.commit()
    date_seq = [x[0] for x in done_set]
    print(date_seq)
    cursor.close()
    #开始模拟交易
    for i in range(1,len(date_seq)):
        cursor = db.cursor()
        #取大盘数据，低于ma5，ma10，ma60，ma60时只卖出不买入
        sql_index = "select * from stock_index a where a.state_dt = '%s' and a.close < a.ma60"%(date_seq[i-1])
        #sql_index = "select * from stock_index a where a.state_dt = '%s' and a.close < a.ma5 and a.close < a.ma10 and a.close < a.ma20 and a.close < a.ma60"%(date_seq[i-1])

        cursor.execute(sql_index)
        done_set_index = cursor.fetchall()
        db.commit()
        cursor.close()
        if len(done_set_index) != 0:
            index_dt = done_set_index[0][0]
            print(str(index_dt) + '   Index Warning')
            Filter.filter_main([], date_seq[i], date_seq[i - 1])
            continue
        #每隔三个月用svm筛选总盘
        if divmod(i-1,60)[1] == 0:
            ans = gp.goodpool_init(date_seq[i-1])
        #每个交易日更新F1千30支股票
        update_pool = []
        remove_pool = []
        stock_pool = []
        cursor = db.cursor()
        sql_update = "select distinct stock_code from good_pool a where a.state_dt = '%s' order by f1 desc limit 30"%(date_seq[i-1])
        cursor.execute(sql_update)
        db.commit()
        done_set_update = cursor.fetchall()
        db.commit()
        update_pool = [x[0] for x in done_set_update]
        sql_remove = "select distinct stock_code from good_pool a where a.state_dt = '%s' order by f1 desc"%(date_seq[i])
        cursor.execute(sql_remove)
        db.commit()
        done_set_remove = cursor.fetchall()
        db.commit()
        remove_pool = [x[0] for x in done_set_remove]
        stock_pool = [x for x in update_pool if x not in remove_pool]
        del done_set_update
        del done_set_remove
        for stock in stock_pool:
            try:
                ans2 = gp.goodpool_update(stock,date_seq[i])
                print('Date : ' + str(date_seq[i]) + ' Update : ' + str(stock))
            except Exception as ex:
                print(ex)
                continue

        #筛选最近两个交易日F1分值最高且有连续买入信号的3只股票
        if i >=2:
            sql_choose = "select distinct stock_code,count(*) from good_pool a where a.predict = 1 and a.state_dt >= '%s' and a.state_dt <='%s' group by a.stock_code having count(*)> 1 order by f1 desc limit 3"%(date_seq[i-2],date_seq[i-1])
            cursor.execute(sql_choose)
            done_stock_new = cursor.fetchall()
            if len(done_stock_new) > 0:
                #stock_new = done_stock_new[0][0]
                stock_new = [x[0] for x in done_stock_new]
                #进入交易环节
                Filter.filter_main(stock_new,date_seq[i],date_seq[i-1])
            else:
                print('No Good Stock : ' + str(date_seq[i]))
        print('Runnig to Date :  ' + str(date_seq[i]))
        cursor.close()
    print('ALL FINISHED!!')
    db.close()




