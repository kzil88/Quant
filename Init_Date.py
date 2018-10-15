import datetime
import tushare as ts
import numpy as np
import pymysql
import calendar

if __name__ == '__main__':

    
    # 建立数据库连接,剔除已入库的部分
    db = pymysql.connect(host='127.0.0.1', user='root', passwd='admin', db='stock', charset='utf8')
    cursor = db.cursor()
    try:
        sql_done_set = "select max(calendarDate) from stock_tradecalall where exchangeCD = 'XSHE'"
        cursor.execute(sql_done_set)
        done_set = cursor.fetchall()
        cur_end_dt = done_set[0][0]
        sql_done_set2 = "select max(calendarDate) from stock_tradecalall where exchangeCD = 'XSHE' and isOpen = 1"
        cursor.execute(sql_done_set2)
        done_set2 = cursor.fetchall()
        cur_open_end_dt = done_set2[0][0]
        start_dt = (datetime.datetime.strptime(cur_end_dt,'%Y-%m-%d') + datetime.timedelta(days=1)).strftime('%Y-%m-%d')
        end_dt = (datetime.datetime.now()).strftime('%Y-%m-%d')

        temp = ts.get_k_data(code='000725', start=start_dt, end=str(end_dt))
        resu = list(np.array(temp.iloc[0:, [0]], dtype=np.str).ravel())
        print(resu)

        while start_dt < end_dt:
            firstDayWeekDay, monthRange = calendar.monthrange(int(str(start_dt[0:4])), int(str(start_dt[5:7])))
            flag_isMonthEnd = 0
            flag_isQuarterEnd = 0
            flag_isWeekEnd = 0
            flag_isYearEnd = 0
            if int(str(start_dt[8:10])) == monthRange:
                flag_isMonthEnd = 1
                if int(str(start_dt[5:7])) in [3,6,8,12]:
                    flag_isQuarterEnd = 1
            if datetime.datetime.strptime(start_dt, '%Y-%m-%d').isoweekday() == 5:
                flag_isWeekEnd = 1
            if str(start_dt[5:10]) == '12-31':
                flag_isYearEnd = 1
            if start_dt != resu[0]:
                sql_insert = "insert into stock_tradecalall(calendarDate,exchangeCD,isMonthEnd,isOpen,isQuarterEnd,isWeekEnd,isYearEnd,prevTradeDate)VALUES('%s','%s','%i','%i','%i','%i','%i','%s')"%(start_dt,'XSHE',flag_isMonthEnd,0,flag_isQuarterEnd,flag_isWeekEnd,flag_isYearEnd,cur_open_end_dt)
                cursor.execute(sql_insert)
                db.commit()
                sql_insert2 = "insert into stock_tradecalall(calendarDate,exchangeCD,isMonthEnd,isOpen,isQuarterEnd,isWeekEnd,isYearEnd,prevTradeDate)VALUES('%s','%s','%i','%i','%i','%i','%i','%s')" % (start_dt, 'XSHG', flag_isMonthEnd, 0, flag_isQuarterEnd, flag_isWeekEnd, flag_isYearEnd,cur_open_end_dt)
                cursor.execute(sql_insert2)
                db.commit()
            else:
                sql_insert = "insert into stock_tradecalall(calendarDate,exchangeCD,isMonthEnd,isOpen,isQuarterEnd,isWeekEnd,isYearEnd,prevTradeDate)VALUES('%s','%s','%i','%i','%i','%i','%i','%s')"%(start_dt,'XSHE',flag_isMonthEnd,1,flag_isQuarterEnd,flag_isWeekEnd,flag_isYearEnd,cur_open_end_dt)
                cursor.execute(sql_insert)
                db.commit()
                sql_insert2 = "insert into stock_tradecalall(calendarDate,exchangeCD,isMonthEnd,isOpen,isQuarterEnd,isWeekEnd,isYearEnd,prevTradeDate)VALUES('%s','%s','%i','%i','%i','%i','%i','%s')" % (start_dt, 'XSHG', flag_isMonthEnd, 1, flag_isQuarterEnd, flag_isWeekEnd, flag_isYearEnd,cur_open_end_dt)
                cursor.execute(sql_insert2)
                db.commit()
                cur_open_end_dt = start_dt
                resu.pop(0)
            start_dt = (datetime.datetime.strptime(start_dt,'%Y-%m-%d') + datetime.timedelta(days=1)).strftime('%Y-%m-%d')
        print('ALL Finished!')



    except Exception as exp:
        print(exp)
