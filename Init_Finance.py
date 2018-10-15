import datetime
import tushare as ts
import numpy as np
import pymysql
import calendar


if __name__ == '__main__':

    # 建立数据库连接,剔除已入库的部分
    db = pymysql.connect(host='127.0.0.1', user='root', passwd='admin', db='stock', charset='utf8')
    cursor = db.cursor()

    year = 2017
    for i in range(4):
        state = ''
        if i == 0:
            state_dt = str(year) + '-06-30'
        elif i == 1:
            state_dt = str(year) + '-09-30'
        elif i == 2:
            state_dt = str(year) + '-12-31'
        elif i == 3:
            state_dt = str(year+1) + '-03-31'
        temp = np.array(ts.get_report_data(year, i+1))
        print('Init_Finance  ===>> 1 of 6')
        for index in temp:
            resu = []
            for j in range(len(index)):
                if str(index[j]) == 'nan' or str(index[j]) == '--':
                    resu.append(0)
                else:
                    resu.append(index[j])
            try:
                sql = "insert into stock_finance(state_dt,stock_code,esp,esp_tb,bvps,roe,epcf,net_profits,profits_tb,distrib,report_dt)values('%s','%s','%.2f','%.2f','%.2f','%.2f','%.2f','%.2f','%.2f','%s','%s')"%(str(state_dt),str(resu[0]),float(resu[2]),float(resu[3]),float(resu[4]),float(resu[5]),float(resu[6]),float(resu[7]),float(resu[8]),str(resu[9]),str(resu[10]))
                cursor.execute(sql)
                db.commit()
            except Exception as exp:
                print(exp)
                continue
        con_sql2 = "select distinct stock_code from stock_finance a where a.state_dt = '%s'"%(state_dt)
        cursor.execute(con_sql2)
        db.commit()
        done_set2 = cursor.fetchall()
        db.commit()
        con2 = [x[0] for x in done_set2]
        temp2 = np.array(ts.get_profit_data(year,i+1))
        print('Init_Finance  ===>> 2 of 6')
        for index2 in temp2:
            resu2 = []
            for l in range(len(index2)):
                if str(index2[l]) == 'nan' or str(index2[l]) == '--':
                    resu2.append(0)
                else:
                    resu2.append(index2[l])
            try:
                if resu2[0] not in con2:
                    sql2 = "insert into stock_finance(state_dt,stock_code,roe,net_profits_ro,gross_profits_ro,net_profits,esp,busy_sr,bips)values('%s','%s','%.2f','%.2f','%.2f','%.2f','%.2f','%.2f','%.2f')" % (
                    str(state_dt), str(resu2[0]),float(resu2[2]),float(resu2[3]),float(resu2[4]),float(resu2[5]),float(resu2[6]),float(resu2[7]),float(resu2[8]))
                    cursor.execute(sql2)
                    db.commit()
                    print('Outer Insert!')
                else:
                    sql2 = "update stock_finance w set w.roe = '%.2f' where w.stock_code = '%s' and w.state_dt = '%s'" % (float(resu2[2]), str(resu2[0]),str(state_dt))
                    cursor.execute(sql2)
                    db.commit()
                    sql2b = "update stock_finance w set w.net_profits_ro = '%.2f' where w.stock_code = '%s' and w.state_dt = '%s'" % (float(resu2[3]), str(resu2[0]),str(state_dt))
                    cursor.execute(sql2b)
                    db.commit()
                    sql2c = "update stock_finance w set w.gross_profits_ro = '%.2f' where w.stock_code = '%s' and w.state_dt = '%s'" % (float(resu2[4]), str(resu2[0]), str(state_dt))
                    cursor.execute(sql2c)
                    db.commit()
                    sql2d = "update stock_finance w set w.net_profits = '%.2f' where w.stock_code = '%s' and w.state_dt = '%s'" % (float(resu2[5]), str(resu2[0]), str(state_dt))
                    cursor.execute(sql2d)
                    db.commit()
                    sql2e = "update stock_finance w set w.esp = '%.2f' where w.stock_code = '%s' and w.state_dt = '%s'" % (float(resu2[6]), str(resu2[0]), str(state_dt))
                    cursor.execute(sql2e)
                    db.commit()
                    sql2f = "update stock_finance w set w.busy_sr = '%.2f' where w.stock_code = '%s' and w.state_dt = '%s'" % (float(resu2[7]), str(resu2[0]), str(state_dt))
                    cursor.execute(sql2f)
                    db.commit()
                    sql2g = "update stock_finance w set w.bips = '%.2f' where w.stock_code = '%s' and w.state_dt = '%s'" % (float(resu2[8]), str(resu2[0]), str(state_dt))
                    cursor.execute(sql2g)
                    db.commit()
            except Exception as ex:
                print(ex)
                continue

        con_sql3 = "select distinct stock_code from stock_finance a where a.state_dt = '%s'" % (state_dt)
        cursor.execute(con_sql3)
        db.commit()
        done_set3 = cursor.fetchall()
        db.commit()
        con3 = [x[0] for x in done_set3]
        temp3 = np.array(ts.get_operation_data(year, i + 1))
        print('Init_Finance  ===>> 3 of 6')
        for index3 in temp3:
            resu3 = []
            for m in range(len(index3)):
                if str(index3[m]) == 'nan' or str(index3[m]) == '--':
                    resu3.append(0)
                else:
                    resu3.append(index3[m])
            try:
                if resu3[0] not in con3:
                    sql3 = "insert into stock_finance(state_dt,stock_code,ar_to,ar_td,inv_to,inv_td,cur_to,cur_td)values('%s','%s','%.2f','%.2f','%.2f','%.2f','%.2f','%.2f')" % (
                        str(state_dt), str(resu3[0]), float(resu3[2]), float(resu3[3]), float(resu3[4]),float(resu3[5]), float(resu3[6]), float(resu3[7]))
                    cursor.execute(sql3)
                    db.commit()
                    print('Outer Insert!')

                else:
                    sql3a = "update stock_finance w set w.ar_to = '%.2f' where w.stock_code = '%s' and w.state_dt = '%s'" % (float(resu3[2]), str(resu3[0]), str(state_dt))
                    cursor.execute(sql3a)
                    db.commit()
                    sql3b = "update stock_finance w set w.ar_td = '%.2f' where w.stock_code = '%s' and w.state_dt = '%s'" % (float(resu3[3]), str(resu3[0]), str(state_dt))
                    cursor.execute(sql3b)
                    db.commit()
                    sql3c = "update stock_finance w set w.inv_to = '%.2f' where w.stock_code = '%s' and w.state_dt = '%s'" % (float(resu3[4]), str(resu3[0]), str(state_dt))
                    cursor.execute(sql3c)
                    db.commit()
                    sql3d = "update stock_finance w set w.inv_td = '%.2f' where w.stock_code = '%s' and w.state_dt = '%s'" % (float(resu3[5]), str(resu3[0]), str(state_dt))
                    cursor.execute(sql3d)
                    db.commit()
                    sql3e = "update stock_finance w set w.cur_to = '%.2f' where w.stock_code = '%s' and w.state_dt = '%s'" % (float(resu3[6]), str(resu3[0]), str(state_dt))
                    cursor.execute(sql3e)
                    db.commit()
                    sql3f = "update stock_finance w set w.cur_td = '%.2f' where w.stock_code = '%s' and w.state_dt = '%s'" % (float(resu3[7]), str(resu3[0]), str(state_dt))
                    cursor.execute(sql3f)
                    db.commit()
            except Exception as ex:
                print(ex)
                continue

        con_sql4 = "select distinct stock_code from stock_finance a where a.state_dt = '%s'" % (state_dt)
        cursor.execute(con_sql4)
        db.commit()
        done_set4 = cursor.fetchall()
        db.commit()
        con4 = [x[0] for x in done_set4]
        temp4 = np.array(ts.get_growth_data(year, i + 1))
        print('Init_Finance  ===>> 4 of 6')
        for index4 in temp4:
            resu4 = []
            for n in range(len(index4)):
                if str(index4[n]) == 'nan' or str(index4[n]) == '--':
                    resu4.append(0)
                else:
                    resu4.append(index4[n])
            try:
                if resu4[0] not in con4:
                    sql4 = "insert into stock_finance(state_dt,stock_code,mbrg,nprg,nav,targ,epsg,seg)values('%s','%s','%.2f','%.2f','%.2f','%.2f','%.2f','%.2f')" % (
                        str(state_dt), str(resu4[0]), float(resu4[2]), float(resu4[3]), float(resu4[4]),float(resu4[5]), float(resu4[6]), float(resu4[7]))
                    cursor.execute(sql4)
                    db.commit()
                    print('Outer Insert!')
                else:
                    sql4a = "update stock_finance w set w.mbrg = '%.2f' where w.stock_code = '%s' and w.state_dt = '%s'" % (float(resu4[2]), str(resu4[0]), str(state_dt))
                    cursor.execute(sql4a)
                    db.commit()
                    sql4b = "update stock_finance w set w.nprg = '%.2f' where w.stock_code = '%s' and w.state_dt = '%s'" % (float(resu4[3]), str(resu4[0]), str(state_dt))
                    cursor.execute(sql4b)
                    db.commit()
                    sql4c = "update stock_finance w set w.nav = '%.2f' where w.stock_code = '%s' and w.state_dt = '%s'" % (float(resu4[4]), str(resu4[0]), str(state_dt))
                    cursor.execute(sql4c)
                    db.commit()
                    sql4d = "update stock_finance w set w.targ = '%.2f' where w.stock_code = '%s' and w.state_dt = '%s'" % (float(resu4[5]), str(resu4[0]), str(state_dt))
                    cursor.execute(sql4d)
                    db.commit()
                    sql4e = "update stock_finance w set w.epsg = '%.2f' where w.stock_code = '%s' and w.state_dt = '%s'" % (float(resu4[6]), str(resu4[0]), str(state_dt))
                    cursor.execute(sql4e)
                    db.commit()
                    sql4f = "update stock_finance w set w.seg = '%.2f' where w.stock_code = '%s' and w.state_dt = '%s'" % (float(resu4[7]), str(resu4[0]), str(state_dt))
                    cursor.execute(sql4f)
                    db.commit()
            except Exception as ex:
                print(ex)
                continue

        con_sql5 = "select distinct stock_code from stock_finance a where a.state_dt = '%s'" % (state_dt)
        cursor.execute(con_sql5)
        db.commit()
        done_set5 = cursor.fetchall()
        db.commit()
        con5 = [x[0] for x in done_set5]
        temp5 = np.array(ts.get_debtpaying_data(year, i + 1))
        print('Init_Finance  ===>> 5 of 6')
        for index5 in temp5:
            resu5 = []
            for o in range(len(index5)):
                if str(index5[o]) == 'nan' or str(index5[o]) == '--':
                    resu5.append(0)
                else:
                    resu5.append(index5[o])
            try:
                if resu5[0] not in con5:
                    sql5 = "insert into stock_finance(state_dt,stock_code,cur_ro,quick_ro,cash_ro,ic_ro,share_ro,share_raise_ro)values('%s','%s','%.2f','%.2f','%.2f','%.2f','%.2f','%.2f')" % (
                        str(state_dt), str(resu5[0]), float(resu5[2]), float(resu5[3]), float(resu5[4]),float(resu5[5]), float(resu5[6]), float(resu5[7]))
                    cursor.execute(sql5)
                    db.commit()
                    print('Outer Insert!')

                else:
                    sql5a = "update stock_finance w set w.cur_ro = '%.2f' where w.stock_code = '%s' and w.state_dt = '%s'" % (float(resu5[2]), str(resu5[0]), str(state_dt))
                    cursor.execute(sql5a)
                    db.commit()
                    sql5b = "update stock_finance w set w.quick_ro = '%.2f' where w.stock_code = '%s' and w.state_dt = '%s'" % (float(resu5[3]), str(resu5[0]), str(state_dt))
                    cursor.execute(sql5b)
                    db.commit()
                    sql5c = "update stock_finance w set w.cash_ro = '%.2f' where w.stock_code = '%s' and w.state_dt = '%s'" % (float(resu5[4]), str(resu5[0]), str(state_dt))
                    cursor.execute(sql5c)
                    db.commit()
                    sql5d = "update stock_finance w set w.ic_ro = '%.2f' where w.stock_code = '%s' and w.state_dt = '%s'" % (float(resu5[5]), str(resu5[0]), str(state_dt))
                    cursor.execute(sql5d)
                    db.commit()
                    sql5e = "update stock_finance w set w.share_ro = '%.2f' where w.stock_code = '%s' and w.state_dt = '%s'" % (float(resu5[6]), str(resu5[0]), str(state_dt))
                    cursor.execute(sql5e)
                    db.commit()
                    sql5f = "update stock_finance w set w.share_raise_ro = '%.2f' where w.stock_code = '%s' and w.state_dt = '%s'" % (float(resu5[7]), str(resu5[0]), str(state_dt))
                    cursor.execute(sql5f)
                    db.commit()
            except Exception as ex:
                print(ex)
                continue

        con_sql6 = "select distinct stock_code from stock_finance a where a.state_dt = '%s'" % (state_dt)
        cursor.execute(con_sql6)
        db.commit()
        done_set6 = cursor.fetchall()
        db.commit()
        con6 = [x[0] for x in done_set6]
        temp6 = np.array(ts.get_cashflow_data(year, i + 1))
        print('Init_Finance  ===>> 6 of 6')
        for index6 in temp6:
            resu6 = []
            for p in range(len(index6)):
                if str(index6[p]) == 'nan' or str(index6[p]) == '--':
                    resu6.append(0)
                else:
                    resu6.append(index6[p])
            try:
                if resu6[0] not in con6:
                    sql6 = "insert into stock_finance(state_dt,stock_code,cf_sales,rate_of_return,cf_nm,cf_lib,cashflow_ro)values('%s','%s','%.2f','%.2f','%.2f','%.2f','%.2f')" % (
                        str(state_dt), str(resu6[0]), float(resu6[2]), float(resu6[3]), float(resu6[4]),float(resu6[5]), float(resu6[6]))
                    cursor.execute(sql6)
                    db.commit()
                    print('Outer Insert!')

                else:
                    sql6a = "update stock_finance w set w.cf_sales = '%.2f' where w.stock_code = '%s' and w.state_dt = '%s'" % (float(resu6[2]), str(resu6[0]), str(state_dt))
                    cursor.execute(sql6a)
                    db.commit()
                    sql6b = "update stock_finance w set w.rate_of_return = '%.2f' where w.stock_code = '%s' and w.state_dt = '%s'" % (float(resu6[3]), str(resu6[0]), str(state_dt))
                    cursor.execute(sql6b)
                    db.commit()
                    sql6c = "update stock_finance w set w.cf_nm = '%.2f' where w.stock_code = '%s' and w.state_dt = '%s'" % (float(resu6[4]), str(resu6[0]), str(state_dt))
                    cursor.execute(sql6c)
                    db.commit()
                    sql6d = "update stock_finance w set w.cf_lib = '%.2f' where w.stock_code = '%s' and w.state_dt = '%s'" % (float(resu6[5]), str(resu6[0]), str(state_dt))
                    cursor.execute(sql6d)
                    db.commit()
                    sql6e = "update stock_finance w set w.cashflow_ro = '%.2f' where w.stock_code = '%s' and w.state_dt = '%s'" % (float(resu6[6]), str(resu6[0]), str(state_dt))
                    cursor.execute(sql6e)
                    db.commit()
            except Exception as ex:
                print(ex)
                continue

    print('ALL Finished!!')


