import datetime
import tushare as ts
import numpy as np
import Choose
import Filter
import pymysql

if __name__ == '__main__':

    # 建测试时间序列（筛选出交易日序列）
    cons = ts.get_apis()
    date_seq = 90
    date_start = []
    date_end = []
    for i in range(date_seq+1):
        time_temp = datetime.datetime.now() - datetime.timedelta(days = date_seq-i)
        end_dt = time_temp.strftime('%Y-%m-%d')
        time_temp2 = datetime.datetime.now() - datetime.timedelta(days = date_seq-i+150)
        start_dt = time_temp2.strftime('%Y-%m-%d')
        #temp_day = ts.bar(code='000725', conn = cons,freq='D',start_date=start_dt,end_date=end_dt,ma=[5,10,20,60],factors=['vr','tor'])
        temp_day = ts.get_k_data(code='000725',start=start_dt,end=end_dt)
        if len(temp_day) > 0:
            #last_deal_day = str(str(temp_day.index[0])[0:10])
            last_deal_day = np.array(temp_day.iloc[temp_day.shape[0]-1:temp_day.shape[0], [0]])[0][0]
            if last_deal_day == end_dt:
                date_start.append(start_dt)
                date_end.append(end_dt)
    print(date_start)
    print(date_end)

    Choose.choose_main(date_start[0], date_end[0])
    cnt = 0
    for i in range(len(date_start)):
        cnt+=1
        if cnt > 10:
            # 建立数据库连接
            db = pymysql.connect(host='127.0.0.1', user='root', passwd='admin', db='stock', charset='utf8')
            cursor = db.cursor()
            sql = 'truncate stock.q1_cs'
            cursor.execute(sql)
            db.commit()
            Choose.choose_main(date_start[i], date_end[i])
            cnt = 0
        Filter.filter_main(date_start[i],date_end[i])

