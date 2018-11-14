import pymysql.cursors
import Deal
import Operator


def filter_main(stock_new,state_dt,predict_dt,year):
    # 建立数据库连接
    db = pymysql.connect(host='127.0.0.1', user='root', passwd='admin', db='stock', charset='utf8')
    cursor = db.cursor()

    #先更新持股天数
    sql_update_hold_days = 'update my_stock_pool w set w.hold_days = w.hold_days + 1'
    cursor.execute(sql_update_hold_days)
    db.commit()

    #先卖出
    deal = Deal.Deal()
    stock_pool_local = deal.stock_pool
    for stock in stock_pool_local:
        sql_predict = "select predict from good_pool a where a.state_dt = '%s' and a.stock_code = '%s'"%(predict_dt,stock)
        cursor.execute(sql_predict)
        done_set_predict = cursor.fetchall()
        predict = 0
        if len(done_set_predict) > 0:
            predict = int(done_set_predict[0][0])
        ans = Operator.sell(stock,state_dt,predict,year)

    #后买入
    #对于已经持仓的股票不再重复买入
    stock_new2 = [x for x in stock_new if x not in stock_pool_local]
    #每只买入股票配仓资金为3万元
    for stock_buy in stock_new2:
        deal_buy = Deal.Deal()
        if deal_buy.cur_money_rest > 30000:
            # sql_ban_pool = "select distinct stock_code from ban_list"
            # cursor.execute(sql_ban_pool)
            # done_ban_pool = cursor.fetchall()
            # ban_list = [x[0] for x in done_ban_pool]
            ans = Operator.buy(stock_buy,state_dt,30000,year)
            break
    db.close()
