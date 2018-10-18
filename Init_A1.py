import datetime
import tushare as ts
import numpy as np
import pymysql
import re

if __name__ == '__main__':

    # 建测试时间序列（筛选出交易日序列）
    cons = ts.get_apis()
    start_dt = '2006-01-01'
    time_temp = datetime.datetime.now() - datetime.timedelta(days=1)
    end_dt = time_temp.strftime('%Y-%m-%d')
    # 取股票代码清单
    src = ts.get_stock_basics()
    code_list = src.index
    code_set = set()
    for i in range(len(code_list)):
        resu = re.search(r'\d+', code_list[i]).string
        code_set.add(resu)
    print('Done:Code_Set')

    # 建立数据库连接,剔除已入库的部分
    db = pymysql.connect(host='127.0.0.1', user='root', passwd='admin', db='stock', charset='utf8')
    cursor = db.cursor()
    sql_done_set = "SELECT distinct stock_code FROM stock_a1 where state_dt = '%s'" % (end_dt)
    #sql_done_set = "SELECT distinct stock_code FROM stock_price_day_list"
    try:
        cursor.execute(sql_done_set)
        done_set = cursor.fetchall()
        for row in done_set:
            code_set.remove(row[0])
            print('Remove: ' + str(row[0]))
        cnt = 1
        total = len(code_set)
        for i in code_set:
            #sql_delete = "delete from stock_price_day_list a where a.stock_code = '%s'"%(i)
            #cursor.execute(sql_delete)
            #db.commit()
            # 股价信息
            try:
                temp_day = ts.bar(code=i, conn = cons,freq='D',start_date='2006-01-01',end_date=end_dt,ma=[5,10,20,30,60],factors=['vr','tor'],adj='qfq')
                print('Seq: ' + str(cnt) + ' of ' + str(total) + '   Code: ' + str(i))
                c_len = temp_day.shape[0]
            except Exception as aa:
                print('No DATA Code: ' + str(i))
                continue
            for j in range(c_len):
                state_dt = str(temp_day.index[c_len-1-j])[0:10]
                resu0 = list(temp_day.ix[c_len-1-j])
                ro = 0.00
                if j != 0:
                    resu_base = list(temp_day.ix[c_len-j])
                    ro = resu0[2]/resu_base[2]
                resu = []
                for k in range(len(resu0)):
                    if str(resu0[k]) == 'nan':
                        resu.append(0)
                    else:
                        resu.append(resu0[k])
                try:
                    sql_insert = "INSERT INTO stock_a1(state_dt,stock_code,open,close,high,low,vol,amount,tor,vr,ma5,ma10,ma20,ma30,ma60,ro) VALUES ('%s', '%s', '%.2f', '%.2f','%.2f','%.2f','%i','%.2f','%.2f','%.2f','%.2f','%.2f','%.2f','%.2f','%.2f','%.4f')" % (state_dt,resu[0],resu[1],resu[2],resu[3],resu[4],resu[5],resu[6],resu[7],resu[8],resu[9],resu[10],resu[11],resu[12],resu[13],ro)
                    cursor.execute(sql_insert)
                    db.commit()
                    #print(str(state_dt) + '   ' + str(resu[0]))
                except Exception as err:
                    #print('Inner SQL Errrrrr   ' + str(err))
                    continue
            cnt += 1

        print('All Finished!')


    except Exception as excp:
        # db.rollback()
        print(str('Errr') + str(i) + str(excp))
    db.close()



