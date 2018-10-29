import pymysql.cursors
import datetime
import tushare as ts

if __name__ == '__main__':
    cons = ts.get_apis()
    ans = ts.get_hist_data(code= 'sh')
    print(ans)

    db = pymysql.connect(host='127.0.0.1', user='root', passwd='admin', db='stock', charset='utf8')
    cursor = db.cursor()
    state_dt = datetime.datetime.now().strftime('%Y-%m-%d')

    stock_pool = ['300500']
    
    for stock in stock_pool:
        print('******************************   股票代码 :  ' + str(stock) + '   Single / AVG(C_Indust)   **************************************')
        sql_select = "select distinct c_indust from stock_classify a where a.stock_code = '%s'"%(stock)
        cursor.execute(sql_select)
        done_set = cursor.fetchall()
        db.commit()
        classify_pool = [x[0] for x in done_set if x[0] != None]
        del done_set
        if len(classify_pool) == 0:
            break
        for index in classify_pool:
            print('==============================   行业分类 :  ' + str(index) + '   ==============================')
            sql_select_single = "select state_dt,esp,esp_tb,bvps,roe,epcf,net_profits,profits_tb,net_profits_ro,gross_profits_ro,busy_sr,bips,ar_to,ar_td,inv_to,inv_td,cur_to,cur_td,mbrg,nprg,nav,targ,epsg,seg,cur_ro,quick_ro,cash_ro,ic_ro,share_ro,share_raise_ro,cf_sales,rate_of_return,cf_nm,cf_lib,cashflow_ro from stock_finance a where a.stock_code = '%s' and a.state_dt <= '%s' order by a.state_dt desc limit 1"%(stock,state_dt)
            cursor.execute(sql_select_single)
            done_set_single = cursor.fetchall()
            db.commit()
            resu_single = list(done_set_single[0])
            state_dt_inner = resu_single[0]
            sql_select_avg = "select avg(esp) c1,avg(esp_tb) c2,avg(bvps) c3,avg(roe) c4,avg(epcf) c5,avg(net_profits) c6,avg(profits_tb) c7,avg(net_profits_ro) c8,avg(gross_profits_ro) c9,avg(busy_sr) c10,avg(bips) c11,avg(ar_to) c12,avg(ar_td) c13,avg(inv_to) c14,avg(inv_td) c15,avg(cur_to) c16,avg(cur_td) c17,avg(mbrg) c18,avg(nprg) c19,avg(nav) c20,avg(targ) c21,avg(epsg) c22,avg(seg) c23,avg(cur_ro) c24,avg(quick_ro) c25,avg(cash_ro) c26,avg(ic_ro) c27,avg(share_ro) c28,avg(share_raise_ro) c29,avg(cf_sales) c30,avg(rate_of_return) c31,avg(cf_nm) c32,avg(cf_lib) c33,avg(cashflow_ro) c34 from stock_finance a where a.stock_code in (select distinct b.stock_code from stock_classify b where b.c_indust = '%s') and a.state_dt = '%s' order by a.state_dt desc limit 1"%(index,state_dt_inner)
            cursor.execute(sql_select_avg)
            done_set_avg = cursor.fetchall()
            db.commit()
            resu_avg = done_set_avg[0]
            print('|-----------------------------------业绩报告（主表）--------------------------------------')
            print('|   每股收益 :   ' + str(resu_single[1]) + '  /  ' + str(resu_avg[0]))
            print('|   每股收益_同比 :   ' + str(resu_single[2]) + '  /  ' + str(resu_avg[1]))
            print('|   每股净资产 :   ' + str(resu_single[3]) + '  /  ' + str(resu_avg[2]))
            print('|   净资产收益率（%） :   ' + str(resu_single[4]) + '  /  ' + str(resu_avg[3]))
            print('|   每股现金流量（元） :   ' + str(resu_single[5]) + '  /  ' + str(resu_avg[4]))
            print('|   净利润（万元） :   ' + str(resu_single[6]) + '  /  ' + str(resu_avg[5]))
            print('|   净利润_同比 :   ' + str(resu_single[7]) + '  /  ' + str(resu_avg[6]))
            print('|-----------------------------------盈利能力--------------------------------------')
            print('|   净利率（%） :   ' + str(resu_single[8]) + '  /  ' + str(resu_avg[7]))
            print('|   毛利率（%） :   ' + str(resu_single[9]) + '  /  ' + str(resu_avg[8]))
            print('|   营业收入（百万元） :   ' + str(resu_single[10]) + '  /  ' + str(resu_avg[9]))
            print('|   每股主营业务收入（元） :   ' + str(resu_single[11]) + '  /  ' + str(resu_avg[10]))
            print('|-----------------------------------营运能力--------------------------------------')
            print('|   应收账款周转率（次） :   ' + str(resu_single[12]) + '  /  ' + str(resu_avg[11]))
            print('|   应收账款周转天数（天） :   ' + str(resu_single[13]) + '  /  ' + str(resu_avg[12]))
            print('|   存货周转率（次） :   ' + str(resu_single[14]) + '  /  ' + str(resu_avg[13]))
            print('|   存货周转天数（天） :   ' + str(resu_single[15]) + '  /  ' + str(resu_avg[14]))
            print('|   流动资产周转率（次） :   ' + str(resu_single[16]) + '  /  ' + str(resu_avg[15]))
            print('|   流动资产周转天数（天） :   ' + str(resu_single[17]) + '  /  ' + str(resu_avg[16]))
            print('|-----------------------------------成长能力--------------------------------------')
            print('|   主营业务收入增长率（%） :   ' + str(resu_single[18]) + '  /  ' + str(resu_avg[17]))
            print('|   净利润增长率（%） :   ' + str(resu_single[19]) + '  /  ' + str(resu_avg[18]))
            print('|   净资产增长率 :   ' + str(resu_single[20]) + '  /  ' + str(resu_avg[19]))
            print('|   总资产增长率 :   ' + str(resu_single[21]) + '  /  ' + str(resu_avg[20]))
            print('|   每股收益增长率 :   ' + str(resu_single[22]) + '  /  ' + str(resu_avg[21]))
            print('|   股东权益增长率seg :   ' + str(resu_single[23]) + '  /  ' + str(resu_avg[22]))
            print('|-----------------------------------偿债能力--------------------------------------')
            print('|   流动比率 :   ' + str(resu_single[24]) + '  /  ' + str(resu_avg[23]))
            print('|   速冻比率 :   ' + str(resu_single[25]) + '  /  ' + str(resu_avg[24]))
            print('|   现金比率 :   ' + str(resu_single[26]) + '  /  ' + str(resu_avg[25]))
            print('|   利息支付倍数 :   ' + str(resu_single[27]) + '  /  ' + str(resu_avg[26]))
            print('|   股东权益比率 :   ' + str(resu_single[28]) + '  /  ' + str(resu_avg[27]))
            print('|   股东权益增长率 share_raise_ro :   ' + str(resu_single[29]) + '  /  ' + str(resu_avg[28]))
            print('|-----------------------------------现金流量--------------------------------------')
            print('|   经营现金净流量 对 销售收入 比率 :   ' + str(resu_single[30]) + '  /  ' + str(resu_avg[29]))
            print('|   资产的经营现金流量 回报率 :   ' + str(resu_single[31]) + '  /  ' + str(resu_avg[30]))
            print('|   经营现金净流量 对 净利润 比率 :   ' + str(resu_single[32]) + '  /  ' + str(resu_avg[31]))
            print('|   经营现金净流量 对 负债 比率 :   ' + str(resu_single[33]) + '  /  ' + str(resu_avg[32]))
            print('|   现金流量 比率 :   ' + str(resu_single[34]) + '  /  ' + str(resu_avg[33]))
            print('|---------------------------------------------------------------------------------')
    print('ALL Finished!!')










