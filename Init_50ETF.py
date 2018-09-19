import datetime
import tushare as ts
import numpy as np
import pymysql
import re
import talib as ta

if __name__ == '__main__':
    db = pymysql.connect(host='127.0.0.1', user='root', passwd='admin', db='stock', charset='utf8')
    cursor = db.cursor()
    cons = ts.get_apis()
    time_temp = datetime.datetime.now() - datetime.timedelta(days=1)
    end_dt = time_temp.strftime('%Y-%m-%d')

    temp_index = ts.bar(code='510050', conn = cons,freq='D',end_date=end_dt,ma=[5,10,20,30,60])
    print(temp_index)

    c_len = temp_index.shape[0]
    #close_list = [x[0] for x in np.array(temp_index.ix[:,[2]])]
    #close_list = close_list[::-1]
    #ma30 = ta.MA(np.array(close_list),timeperiod=30)
    #ma60 = ta.MA(np.array(close_list),timeperiod=60)
    for j in range(c_len):
        state_dt = str(temp_index.index[c_len-1-j])[0:10]
        resu0 = list(temp_index.ix[c_len-1-j])
        resu = []
        ro = 0.00
        p_change = 0.00
        if j != 0:
            resu_base = list(temp_index.ix[c_len - j])
            ro = resu0[2] / resu_base[2]
            p_change = resu0[2]-resu_base[2]
        resu = []
        for k in range(len(resu0)):
            if str(resu0[k]) == 'nan':
                resu.append(0)
            else:
                resu.append(resu0[k])
        # if str(ma30[j]) == 'nan':
        #     resu.append(float(0))
        # else:
        #     resu.append(float(ma30[j]))
        # if str(ma60[j]) == 'nan':
        #     resu.append(float(0))
        # else:
        #     resu.append(float(ma60[j]))
        try:
            sql_insert = "INSERT INTO stock_index(state_dt,stock_code,open,close,high,low,vol,amount,ma5,ma10,ma20,ma30,ma60,ro,p_change) VALUES ('%s', '%s', '%.2f', '%.2f','%.2f','%.2f','%i','%.2f','%.2f','%.2f','%.2f','%.2f','%.4f','%.2f')" % (str(state_dt),str('50ETF'),float(resu[1]),float(resu[2]),float(resu[3]),float(resu[4]),int(resu[5]),float(resu[7]),float(resu[8]),float(resu[9]),float(resu[10]),float(resu[11]),float(ro),float(p_change))
            cursor.execute(sql_insert)
            db.commit()
        except Exception as err:
            print(err)
            continue

    print('All Finished!')
    db.close()


