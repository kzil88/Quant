import datetime
import tushare as ts
import numpy as np
import pymysql
import calendar

if __name__ == '__main__':

    
    # 建立数据库连接,剔除已入库的部分
    db = pymysql.connect(host='127.0.0.1', user='root', passwd='admin', db='stock', charset='utf8')
    cursor = db.cursor()
    temp1 = ts.get_concept_classified()
    temp1mid = np.array(temp1)
    print('Init_Classify  ===>> 1 of 9. Concept')
    for j in range(len(temp1mid)):
        resu1 = temp1mid[j]
        print('Init_Classify  ===>> 1 of 9. Concept   ' + str(j+1) + '  of total ' + str(len(temp1mid)))
        try:
            sql = "insert into stock_classify(stock_code,c_concept)values('%s','%s')"%(str(resu1[0]),str(resu1[2]))
            cursor.execute(sql)
            db.commit()
        except Exception as ex:
            print(ex)
            continue

    con_sql = "select distinct stock_code from stock_classify"
    cursor.execute(con_sql)
    done_set = cursor.fetchall()
    db.commit()
    con = [x[0] for x in done_set]
    temp2 = ts.get_industry_classified()
    temp2mid = np.array(temp2)
    print('Init_Classify  ===>> 2 of 9. Industry')
    for i in range(len(temp2mid)):
        resu2 = temp2mid[i]
        try:
            if resu2[0] not in con:
                sql2 = "insert into stock_classify(stock_code,c_indust)values('%s','%s')" % (str(resu2[0]), str(resu2[2]))
                cursor.execute(sql2)
                db.commit()
            else:
                sql2 = "update stock_classify w set w.c_indust = '%s' where w.stock_code = '%s'"%(str(resu2[2]),str(resu2[0]))
                cursor.execute(sql2)
                db.commit()
        except Exception as ex:
            print(ex)
            continue

    con_sql2 = "select distinct stock_code from stock_classify"
    cursor.execute(con_sql2)
    done_set2 = cursor.fetchall()
    db.commit()
    con2 = [x[0] for x in done_set2]
    temp3 = ts.get_area_classified()
    temp3mid = np.array(temp3)
    print('Init_Classify  ===>> 3 of 9. Area')
    for k in range(len(temp3mid)):
        resu3 = temp3mid[k]
        try:
            if resu3[0] not in con2:
                sql3 = "insert into stock_classify(stock_code,c_area)values('%s','%s')" % (str(resu3[0]), str(resu3[2]))
                cursor.execute(sql3)
                db.commit()
            else:
                sql3 = "update stock_classify w set w.c_area = '%s' where w.stock_code = '%s'"%(str(resu3[2]),str(resu3[0]))
                cursor.execute(sql3)
                db.commit()
        except Exception as ex:
            print(ex)
            continue

    con_sql3 = "select distinct stock_code from stock_classify"
    cursor.execute(con_sql3)
    done_set3 = cursor.fetchall()
    db.commit()
    con3 = [x[0] for x in done_set3]
    temp4 = ts.get_sme_classified()
    temp4mid = np.array(temp4)
    print('Init_Classify  ===>> 4 of 9. Size')
    for l in range(len(temp4mid)):
        resu4 = temp4mid[l]
        try:
            if resu4[0] not in con3:
                sql4 = "insert into stock_classify(stock_code,c_size)values('%s','%s')" % (str(resu4[0]), str(1))
                cursor.execute(sql4)
                db.commit()
            else:
                sql4 = "update stock_classify w set w.c_size = '%s' where w.stock_code = '%s'" % (str(1), str(resu4[0]))
                cursor.execute(sql4)
                db.commit()
        except Exception as ex:
            print(ex)
            continue

    con_sql4 = "select distinct stock_code from stock_classify"
    cursor.execute(con_sql4)
    done_set4 = cursor.fetchall()
    db.commit()
    con4 = [x[0] for x in done_set4]
    temp5 = ts.get_gem_classified()
    temp5mid = np.array(temp5)
    print('Init_Classify  ===>> 5 of 9. Career')
    for m in range(len(temp5mid)):
        resu5 = temp5mid[m]
        try:
            if resu5[0] not in con4:
                sql5 = "insert into stock_classify(stock_code,c_career)values('%s','%s')" % (str(resu5[0]), str(1))
                cursor.execute(sql5)
                db.commit()
            else:
                sql5 = "update stock_classify w set w.c_career = '%s' where w.stock_code = '%s'" % (str(1), str(resu5[0]))
                cursor.execute(sql5)
                db.commit()
        except Exception as ex:
            print(ex)
            continue

    con_sql5 = "select distinct stock_code from stock_classify"
    cursor.execute(con_sql5)
    done_set5 = cursor.fetchall()
    db.commit()
    con5 = [x[0] for x in done_set5]
    temp6 = ts.get_st_classified()
    temp6mid = np.array(temp6)
    print('Init_Classify  ===>> 6 of 9. ST')
    for n in range(len(temp6mid)):
        resu6 = temp6mid[n]
        try:
            if resu6[0] not in con5:
                sql6 = "insert into stock_classify(stock_code,c_st)values('%s','%s')" % (str(resu6[0]), str(1))
                cursor.execute(sql6)
                db.commit()
            else:
                sql6 = "update stock_classify w set w.c_st = '%s' where w.stock_code = '%s'" % (str(1), str(resu6[0]))
                cursor.execute(sql6)
                db.commit()
        except Exception as ex:
            print(ex)
            continue

    con_sql6 = "select distinct stock_code from stock_classify"
    cursor.execute(con_sql6)
    done_set6 = cursor.fetchall()
    db.commit()
    con6 = [x[0] for x in done_set6]
    temp7 = ts.get_hs300s()
    temp7mid = np.array(temp7)
    print('Init_Classify  ===>> 7 of 9. hs300')
    for o in range(len(temp7mid)):
        resu7 = temp7mid[o]
        try:
            if resu7[0] not in con6:
                sql7 = "insert into stock_classify(stock_code,hs300,hs300_weight)values('%s','%s','%.2f')" % (str(resu7[0]), str(1),float(resu7[3]))
                cursor.execute(sql7)
                db.commit()
            else:
                sql7a = "update stock_classify w set w.hs300 = '%s' where w.stock_code = '%s'" % (str(1), str(resu7[0]))
                cursor.execute(sql7a)
                db.commit()
                sql7b = "update stock_classify w set w.hs300_weight = '%.2f' where w.stock_code = '%s'" % (float(resu7[3]), str(resu7[0]))
                cursor.execute(sql7b)
                db.commit()
        except Exception as ex:
            print(ex)
            continue

    con_sql7 = "select distinct stock_code from stock_classify"
    cursor.execute(con_sql7)
    done_set7 = cursor.fetchall()
    db.commit()
    con7 = [x[0] for x in done_set7]
    temp8 = ts.get_sz50s()
    temp8mid = np.array(temp8)
    print('Init_Classify  ===>> 8 of 9. SZ50')
    for p in range(len(temp8mid)):
        resu8 = temp8mid[p]
        try:
            if resu8[0] not in con7:
                sql8 = "insert into stock_classify(stock_code,sz50)values('%s','%s')" % (str(resu8[0]), str(1))
                cursor.execute(sql8)
                db.commit()
            else:
                sql8 = "update stock_classify w set w.sz50 = '%s' where w.stock_code = '%s'" % (str(1), str(resu8[0]))
                cursor.execute(sql8)
                db.commit()
        except Exception as ex:
            print(ex)
            continue

    con_sql8 = "select distinct stock_code from stock_classify"
    cursor.execute(con_sql8)
    done_set8 = cursor.fetchall()
    db.commit()
    con8 = [x[0] for x in done_set8]
    temp9 = ts.get_zz500s()
    temp9mid = np.array(temp9)
    print('Init_Classify  ===>> 9 of 9. ZZ500')
    for q in range(len(temp9mid)):
        resu9 = temp9mid[q]
        try:
            if resu9[0] not in con8:
                sql9 = "insert into stock_classify(stock_code,zz500)values('%s','%s')" % (str(resu9[0]), str(1))
                cursor.execute(sql9)
                db.commit()
            else:
                sql9 = "update stock_classify w set w.zz500 = '%s' where w.stock_code = '%s'" % (str(1), str(resu9[0]))
                cursor.execute(sql9)
                db.commit()
        except Exception as ex:
            print(ex)
            continue



    print('ALL Finished!!')


