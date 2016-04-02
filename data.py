#-*- coding: utf-8 -*-，
#coding = utf-8
import init
import util
import numexpr
import time
import datetime
from time import sleep
import random
import pandas as pd

# ##############################################################
#数据类
class Data():
    def __init__(self):
        self.allStocksBasicInfo = None  #所有的股票基本信息，从datayest_equ中查取的
        self.allFundBasicInfo = None    #所有基金基本信息
        self.allExistTables = set([])    #所有已经存在的表信息
        self.remainAll = set([])    #剩余待查取的股票代码list
        self.fails = set([]) #下载失败的股票ticker
        self._config = None  #配置信息
        self.qfqFactor = None #前复权因子

    # 获取全部股票基本信息
    def FetchAllStocksBasicInfo(self):
        print '开始FetchAllStocksBasicInfo'
        eq = init.ts.Equity()
        df = eq.Equ(equTypeCD='A', listStatusCD='L', field='')
        df.to_sql('datayest_equ',init.getEngine(),if_exists='replace',index=False)
        self.allStocksBasicInfo = df

    # 获取全部基金基本信息
    def FetchAllFundBasicInfo(self):
        print '开始FetchAllFundBasicInfo'

    # 获取所有股票的前复权因子
    def FetchAllStockQfqFactor(self):
        print '开始FetchAllStockQfqFactor'
        cur = init.getCursor()
        conn = init.getConn()
        #先删除表
        sql = init.g_dropSql % init.g_datayest_mktadjf.lower()
        cur.execute(sql)
        conn.commit()

        st = init.ts.Market()
        maxids = 50
        idx = 0
        l = len(self.allStocksBasicInfo)
        while(idx<l):
            stocks = pd.np.array( (self.allStocksBasicInfo)[idx:idx+maxids] )
            str_ticker = ','.join( ('%06d' % a[1]) for a in stocks )
            idx = idx+maxids
            print 'FetchAllStockQfqFactor %s' % str_ticker
            df = st.MktAdjf(ticker=str_ticker)
            df.to_sql(init.g_datayest_mktadjf.lower(),init.getEngine(),if_exists='append',index=False)
        self.qfqFactor = df

    #初始化数据
    def InitData(self):
        # 初始化config
        df = pd.read_sql( init.g_selectSql % '_config', init.g_conn)
        if df.shape[0]==1:
            self._config = df.iloc[0]

        #如果今天还没有更新过了，才FetchBasic
        bn = False  #是否需要下载数据
        lastDate = datetime.datetime.strptime(self._config['last_daysdata_update_date'], '%Y%m%d')
        today = datetime.datetime.now()
        if today.hour<init.g_fetch_time :
            today = today - datetime.timedelta(hours=24)
        bn = lastDate.date()<today.date()
        if bn:
            self.FetchAllStocksBasicInfo()
            self.FetchAllStockQfqFactor()
            self.FetchAllFundBasicInfo()

        # 初始化股票基本信息表
        if self.allStocksBasicInfo is None :
            self.allStocksBasicInfo = pd.read_sql( init.g_selct_datayest_equSql, init.getConn())
        if self.qfqFactor is None :
            self.qfqFactor = pd.read_sql(init.g_select_datayest_mktadjf, init.getConn())

        # 初始化已存在的表名
        cur = init.getCursor()
        cur.execute('show tables')
        allTmp = cur.fetchall()
        for a in allTmp:
          self.allExistTables.add(a[0])
        # 过滤已经爬取的数据
        kk = self.allStocksBasicInfo.set_index('ticker')['secShortName'].to_dict()
        for k in kk:
            tableName = '%s%06d' % (init.g_mktequd,k)
            bFind = tableName in self.allExistTables
            if bFind==False:
                self.remainAll.add(k)

        # 更新历史日线数据
        self.FetchAllHistoryDayData()
        # 更新指数日线数据
        self.FetchAllIdxDaysData()

        # 需要按日期补充下载数据
        if bn:
            #根据上次最后更新日期，开始FetchDayData
            #根据现在时间，看是否下午18点之前还是之后，生成2个日期差的交易日列表，FetchDayData这些交易日
            count = (today.date()-lastDate.date()).days
            while count > 0:
                theDate = (today-datetime.timedelta(days=count-1)).strftime('%Y%m%d')
                self.FetchDayData(theDate)
                self.FetchIdxDaysDate(theDate)
                count = count-1

        #下载完成后，更新config表
        sql = init.g_update_config % (today.strftime('%Y%m%d'), lastDate.strftime('%Y%m%d'))
        cur.execute(sql)
        init.getConn().commit()
        cur.close()

    #获取股票tick的历史日线数据,beg不填写的话为第一天上市的日期（%Y%m%d格式），end不填写的话为最新日期
    def FetchHistoryDayData(self,ticker,beg=None,end=None,st=init.ts.Market()):
        if beg==None:
            beg = '19901010'#time.strftime('%Y%m%d', time.strptime(a[10], '%Y-%m-%d')) #因为1989还没有A股
        if end==None:
            end = time.strftime('%Y%m%d', time.localtime())
        print '查询%s从%s到%s的数据' % (ticker, beg, end)
        bGetData = False
        nLoop = 0

        # 创建表
        tableName = '%s%s' % (init.g_mktequd,ticker)
        sql = init.g_create_table_mktequd % tableName
        init.getCursor().execute(sql)
        init.getConn().commit()

        # 循环3次获取数据
        while nLoop < 3 and bGetData == False:  # 循环3次
            df = st.MktEqud(ticker=ticker, beginDate=beg, endDate=end)
            bGetData = df.shape[0] > 0
            if bGetData == False:
                nLoop = nLoop + 1
                sleep(random.uniform(0.5, 1))
            else:  # 获取成功，保存到数据库中
                df.to_sql(tableName, init.getEngine(), if_exists='append',index=False)
                return True
        if bGetData == False:
            self.fails.add(ticker)
            print '未获取到%s' % ticker
        return False

    #获取所有股票的历史日线数据
    def FetchAllHistoryDayData(self):
        for a in self.remainAll:
            tick = ('%06d' % a)
            self.FetchHistoryDayData(tick)

    #获取指定日期的日行情数据，并把数据更新到各张表中
    def FetchDayData(self,dateStr):
        st = init.ts.Market()
        df = st.MktEqud(tradeDate=dateStr)
        bGetData = df.shape[0]>0
        if bGetData:
            print '成功获取%s的日行情数据' % dateStr
        else:
            print '未获取到%s的日行情数据' % dateStr
        #将今日行情数据插入到各张表中
        if bGetData:
            self.UpdateDaysData(dateStr,df)

    #更新指定日行情数据到各张表中
    def UpdateDaysData(self,tradeDate,df):
        cur = init.getCursor()
        for i in range(0,df.shape[0]):    #(df.shape[0]):
            dd = df.iloc[i] #获取一行
            tableName = '%s%06d' % (init.g_mktequd,dd[1])
            #判断表是否存在
            if tableName in self.allExistTables:
                try:
                    insertSql = init.g_insert_table_mktequd % (tableName,dd[0],dd[1],dd[2],dd[3],dd[4],dd[5],dd[6],dd[7],dd[8],dd[9],dd[10],dd[11],dd[12],dd[13],dd[14],dd[15],dd[16],dd[17],dd[18],dd[19],dd[20],dd[21])
                    newSql = insertSql.replace('\'nan\'','NULL')
                    newSql = newSql.replace('nan','NULL')
                    print newSql
                    cur.execute(newSql)
                except init.MySQLdb.Error,e:
                    print "Mysql Error %d: %s" % (e.args[0], e.args[1])
        init.getConn().commit()
        cur.close()

    # 获取指数日行情
    def FetchAllIdxDaysData(self):
        st = init.ts.Market()
        df = pd.read_sql( init.g_select_idx, init.g_conn)   #指数基本列表
        for i in range(0,df.shape[0]):
            dd = df.iloc[i]
            idxTableName = ('%s%s' % (init.g_mktidxd,dd['ticker'])).lower()
            # 该指数表不存在才需要完全下载
            if (idxTableName in self.allExistTables) == False:
                try:
                    sql = init.g_create_table_mktidxd % idxTableName
                    init.getCursor().execute(sql)
                    init.getConn().commit()
                    dm = st.MktIdxd(ticker=dd['ticker'])
                    dm.to_sql( idxTableName,init.getEngine(),if_exists='append',index=False )
                    print 'FetchAllIdxDaysData:%s' % idxTableName
                    sleep(0.5)
                except init.MySQLdb.Error,e:
                    print "Mysql Error %d: %s" % (e.args[0], e.args[1])

    # 获取指定日期的指数日行情
    def FetchIdxDaysDate(self,dateStr):
        st = init.ts.Market()
        df = st.MktIdxd(tradeDate = dateStr)
        bGetData = df.shape[0]>0
        if bGetData:
            print '成功获取%s的指数日行情数据' % dateStr
        else:
            print '未获取到%s的指数日行情数据' % dateStr
        #将指数日行情数据插入到各张表中
        if bGetData:
            self.UpdateIdxDaysData(dateStr,df)

    # 根据下载的指数日线数据，插入到各张表
    def UpdateIdxDaysData(self,dateStr,df):
        # df.fillna(None)   # ValueError: must specify a fill method or value
        cur = init.getCursor()
        for i in range(0,df.shape[0]):    #(df.shape[0]):
            dd = df.iloc[i]
            idxTableName = ('%s%s' % (init.g_mktidxd,dd['ticker'])).lower()
            # 该指数表存在    (None if pd.isnull(dd['exchangeCD']) else dd['exchangeCD'])
            if (idxTableName in self.allExistTables):
                try:
                    insertSql = init.g_insert_table_mktidxd % \
                                (idxTableName,dd['indexID'],dd['ticker'],dd['porgFullName'],dd['secShortName'],dd['exchangeCD'] ,
                                 dd['tradeDate'],dd['preCloseIndex'],dd['openIndex'],dd['lowestIndex'],
                                 dd['highestIndex'],dd['closeIndex'],dd['turnoverVol'],dd['turnoverValue'],dd['CHG'],dd['CHGPct'])
                    newSql = insertSql.replace('\'nan\'','NULL')
                    newSql = newSql.replace('nan','NULL')
                    print newSql
                    cur.execute(newSql)
                except init.MySQLdb.Error,e:
                    print "Mysql Error %d: %s" % (e.args[0], e.args[1])
        init.getConn().commit()
        cur.close()

    # 统计大V荐股准确性(针对石老A的超短群)
    def CountTuijianStockData(self):
        #遍历_tuijian_stock表，统计1天涨幅以及成功率，2天涨幅以及成功率，3天涨幅以及成功率
        df = pd.read_sql( 'select * from _tuijian_stock', init.g_conn)
        cur = init.getCursor()
        conn = init.getConn()
        for i in range(0,df.shape[0]):
            dd = df.iloc[i]
            if dd['percent_1day']==None :
                # 根据时间和id计算涨幅
                tableName = ('%s%s' % (init.g_mktequd,dd['tuijian_stock_id']))
                sql = 'select * from %s where tradeDate=\'%s\'' %(tableName,time.strftime('%Y-%m-%d', time.strptime(dd['tuijian_time'], '%Y%m%d')))
                print sql
                n=cur.execute(sql)
                if n==1:
                    the = cur.fetchall()
                    percent = (the[0][11]-the[0][8])/the[0][7]
                    # UPDATE `_tuijian_stock` SET `percent_1day`='0.1' WHERE (`name`='JOJO') AND (`tuijian_stock_id`='002074') AND (`tuijian_time`='20160314')
                    sql =  'UPDATE `_tuijian_stock` SET `percent_1day`=\'%s\' WHERE (`name`=\'%s\') AND (`tuijian_stock_id`=\'%s\') AND (`tuijian_time`=\'%s\')' % (percent,dd['name'],dd['tuijian_stock_id'],dd['tuijian_time'])
                    print sql
                    cur.execute(sql)
            if dd['percent_2day']==None :
                pass
            if dd['percent_3day']==None :
                pass
        conn.commit()
        cur.close()

    #得到股票价格（不复权）
    def GetPrice(self,ticker,_time,bForward=True):
        count = 0
        price = -1
        qfq = 1
        while _time.date()<=datetime.datetime.now().date():
            # 向前or向后追溯
            ntime = _time+datetime.timedelta(days=count) if bForward else datetime.datetime.now()-datetime.timedelta(days=count)
            df = pd.read_sql('select openPrice from %s%s where tradeDate=\'%s\'' % (init.g_mktequd,'%06d' % ticker, ntime.strftime('%Y-%m-%d')),init.getConn())
            if df.shape[0]==1:
                price = df.iloc[0]['openPrice']
                qfq = self.GetQfq(ticker,ntime)
                break
            else:
                count = count+1
        return  price

    #获取前复权因子
    def GetQfq(self,ticker,_time=datetime.datetime.now()):
        # 根据时间获取前复权因子
        qfq = 1 #前复权因子
        df = self.qfqFactor.query('ticker==%s' % ticker)
        for i in range(0,df.shape[0]):
            dd = df.iloc[i]
            time_ex = datetime.datetime.strptime(dd['exDivDate'], "%Y-%m-%d")
            time_end = datetime.datetime.strptime(dd['endDate'], "%Y-%m-%d")
            if _time.date()>=time_end.date() and _time.date()<=time_ex.date():
                qfq = dd['adjFactor']
                break
        return qfq

    # 计算前复权价格
    def CalcQfqPrice(self,ticker,_time):
        return self.GetPrice(ticker,_time)*self.GetQfq(ticker,_time)

    #Test 找出存在的表，但是在datayest_equ中不存在
    def test(self):
        bb = list()
        kk = self.allStocksBasicInfo.set_index('ticker')['secShortName'].to_dict()

        for a in self.allExistTables:
            bFind = False
            num = filter(str.isdigit,a.encode('ascii'))
            if num!='' and kk.has_key(int(num))==False:
                bb.append(a)
        print(bb)

    # 找出在datayest_equ中存在但是未保存下来的表
    def test1(self):
        bb = list()
        kk = self.allStocksBasicInfo.set_index('ticker')['secShortName'].to_dict()
        for k in kk:
            tableName = '%s%06d' % (init.g_mktequd,k)
            bFind = tableName in self.allExistTables
            if bFind==False:
                bb.append(tableName)
        print bb

    # tes
    def test2(self):
        st = init.ts.Market()

        today = datetime.datetime.now()
        if today.hour<init.g_fetch_time :
            today = today - datetime.timedelta(hours=24)
        for i in range(0,30):
            today = today - datetime.timedelta(hours=24)
            try:
                df = st.MktIdxd(tradeDate=today.strftime('%Y%m%d'))
            except e:
                print "Error %d: %s" % (e.args[0], e.args[1])
            sleep(0.5)
            print i

    # 修改之前的表结构。
    # 1，如果日详情表（指数日行情表）不存在，新建表格式，并调用to_sql存储。
    # 2，已结存在的日行情表（指数行情表），修改表格式。
    def test3(self):
        # ALTER TABLE `mktidxd801030` MODIFY COLUMN `exchangeCD`  text NULL AFTER `secShortName`
        # 初始化已存在的表名
        cur = init.getCursor()
        cur.execute('show tables')
        allTmp = cur.fetchall()
        for a in allTmp:
            if (a[0].find(init.g_mktequd)!=-1 or a[0].find(init.g_mktidxd)!=-1):
                try:
                    sql = 'ALTER TABLE `%s` DROP COLUMN `index`' % a[0]
                    print sql
                    cur.execute(sql)
                except init.MySQLdb.Error,e:
                    print "Mysql Error %d: %s" % (e.args[0], e.args[1])

        # for a in allTmp:
        #     if a[0].find(init.g_mktidxd)!=-1 :
        #         # sql = 'DELETE FROM `%s` WHERE (`tradeDate`=\'2016-03-24\')' % a[0]
        #         # print sql
        #         # cur.execute(sql)
        #         # sql = 'DELETE FROM `%s` WHERE (`tradeDate`=\'2016-03-25\')' % a[0]
        #         # print sql
        #         # cur.execute(sql)
        #         # sql = 'ALTER TABLE `%s` MODIFY COLUMN `tradeDate`  varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL, ADD PRIMARY KEY (`tradeDate`)' % a[0]
        #         # print sql
        #         # cur.execute(sql)
        #         try:
        #         #     sql = 'ALTER TABLE `%s` MODIFY COLUMN `exchangeCD`  text NULL,\
        #         # MODIFY COLUMN `tradeDate`  varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL AFTER `exchangeCD`,\
        #         # ADD PRIMARY KEY (`tradeDate`)' % a[0]
        #             sql = ' ALTER TABLE `%s` \
        #         MODIFY COLUMN `ticker`  varchar(20) NULL DEFAULT NULL AFTER `indexID`,\
        #         MODIFY COLUMN `turnoverVol`  double NULL DEFAULT NULL AFTER `closeIndex`,\
        #         MODIFY COLUMN `turnoverValue`  double NULL DEFAULT NULL AFTER `turnoverVol`,\
        #         MODIFY COLUMN `tradeDate`  varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,\
        #         ADD PRIMARY KEY (`tradeDate`)' % a[0]
        #             print sql
        #             cur.execute(sql)
        #         except init.MySQLdb.Error,e:
        #             print "Mysql Error %d: %s" % (e.args[0], e.args[1])
        init.getConn().commit()

    # 测试函数
    def test4(self):
        # st = init.ts.Market()
        # df = st.TickRTSnapshot(securityID='000001.XSHG')  #没有权限   2016-4-1 19:18:21
        # nowDf = init.ts.get_today_all()
        # nowDf.to_sql('_today_all',getEngine(),if_exists='replace',index=False)
        nowDf = pd.read_sql('select * from _today_all',init.getConn())
        #遍历表中的股票id，找出实时数据：今天涨幅，累计涨幅，实时价格
        df = pd.read_sql( ('select * from %s' % init.g_tuijian_stock_v16_ziming), init.getConn())
        df['accumPercent'] = pd.Series(None,index=df.index)
        df['todayPercent'] = pd.Series(None,index=df.index)
        for i in range(0,df.shape[0]):
            dd = df.iloc[i]
            #计算推荐时价格（前复权）
            tuijian_price = self.CalcQfqPrice(int(dd['tuijian_stock_id']),datetime.datetime.strptime(dd['tuijian_time'], "%Y%m%d"))
            #查取现在实时价格。
            nowPrice = -1
            nowPriceDf = nowDf[nowDf.code==dd['tuijian_stock_id']]
            if nowPriceDf.shape[0]==1:
                nowPrice = nowPriceDf['trade']*self.GetQfq(dd['tuijian_stock_id'])
                df.at[i,'todayPercent'] = nowPriceDf['changepercent']   #得到今天涨幅
            else:   #没查到，说明今天停牌了
                dt = pd.read_sql((init.g_select_actPreClosePrice % dd['tuijian_stock_id']), init.getConn())
                nowPrice = dt.iloc[0]['actPreClosePrice']*self.GetQfq(dd['tuijian_stock_id'])   #需要判断dt不存在的情况吗？实际上不存在dt为空

            #得到累计涨幅和今天涨幅
            df.at[i,'accumPercent'] = (nowPrice-tuijian_price)/tuijian_price*100
        print df

########################################################################

if __name__ == '__main__':
    init.init()
    data = Data()
    data.InitData()

    # data.test2()
    # data.test3()
    data.test4()

    # data.FetchDayData('20160314')
    # data.FetchDayData('20160315')

    # data.FetchAllHistoryDayData()
    # data.UpdateDaysData(util.getTodayYYmmddStr())

    # data.CountTuijianStockData()

    # print data.remainAll
    # data.FetchHistoryDayData('603999','20151210')
    # data.FetchAllHistoryDayData()
    # data.FetchDayData('20160311')
    # data.UpdateDaysData('20160311')
    # data.test()
    # data.test1()

    # 日行情的处理流程：
    # 1，重新更新datayest_equ
    # 2. FetchAllHistoryDayData会会新股票创建出新表mktequdXXXXXX，并会调用AddPrimaryKey处理好表结构
    # 3. FetchDayData获取到今日行情，并调用UpdateDaysData更新今日行情到各张mktequdXXXXXX中

# 2016-3-25 16:31:15 重大修改
# index字段没什么用，全部去掉，
#

######################################################################################################################


#新浪财经
# 保存全部股票基本信息
# df = ts.get_stock_basics()
# df.to_sql('stock_basics',engine,if_exists='append')

# df = ts.get_tick_data('600848',date='2016-01-15')

# 获取所有股票的前复权数据
# n = cur.execute(r' select * from stock_basics')
# all = cur.fetchall()
# n = cur.execute(r' select ticker from datayest_equ')
# all = cur.fetchall()

# idx=0
# while idx<3:
#     df = ts.get_h_data(all[idx][0], start='2015-12-10', end='2016-01-25')
#     df.to_sql('h_data'+str(all[idx][0]),engine,if_exists='append')
#     idx = idx+1

# 获取今日涨停板
# now = datetime.datetime.now()
# str = now.strftime("%Y%m%d")
# st = ts.Market()
# df = st.MktEqud(tradeDate=str)
# dd = df.query('(closePrice-actPreClosePrice)/actPreClosePrice>0.099 & closePrice==highestPrice')

# now = datetime.datetime.now()
# str = now.strftime("%Y%m%d")
# st = ts.Market()
# df = st.MktEqudM(ticker='000001',monthEndDate=str)
# df.to_sql('datayest_000001',engine,if_exists='append')

# 获取全部股票数据，用线程？？
# 结合前复权因子。。


########################################
# 获取全部股票基本信息
# eq = ts.Equity()
# df = eq.Equ(equTypeCD='A', listStatusCD='L', field='')
# df.to_sql('datayest_Equ',engine,if_exists='append')
# 沪深融资融券每日汇总信息
# df = eq.FstTotal()
# df.to_sql('datayest_FstTotal',engine,if_exists='append')

########################################
# fd = ts.Subject()
# df = fd.SocialDataXQ()
# # df.to_sql('datayest_SocialDataXQ',engine,if_exists='append')
# print df.head(5)

# Empty DataFrame
# Columns: [-403:Need privilege]
# Index: []
# [Finished in 3.4s]

#获取历史某一日股票行情数据，包括了停牌股票（停牌的报价都是0）
# st = ts.Market()
# df = st.MktEqud(tradeDate='20150917')

#获取某只股票的历史日线信息
#demoTicker=''
# st = ts.Market()
# df = st.MktEqud(ticker='000001',beginDate='20130601',endDate='20160308')
# df.to_sql('000001',engine,if_exists='append')
#好的，直接用通联的复权因子就可以计算出前复权数据了

##########################################################################################

#获取所有股票的历史日线数据（不复权）
# n = cur.execute(r' select * from datayest_equ')
# all = cur.fetchall()
# print '共有 %s支股票' %n
#
# n = cur.execute(r'show tables')
# hasDownload = cur.fetchall()
# print '已经爬取了%s支' %n
#
# #过滤已经爬取的数据
# all1 = list()
# for a in all:
#     tick = ('%06d' % a[2])
#     bn = False
#     for h in hasDownload:
#         if h[0].find(tick)!=-1:
#             bn = True
#             break
#     if bn==False:
#         all1.append(a)
# all = all1
#
# print '还需要爬取%s支' % len(all)
#
# fails = set([])

# #开10个线程，从all中取股票代码，每取出一个，进行获取数据，并保存到数据库表的工作。
# import time
# from threading import Thread
# from Queue import Queue
# from time import sleep
# import random
# #q是任务队列
# #NUM是并发线程总数
# #JOBS是有多少任务
# q = Queue()
# NUM = 4
# JOBS = 10
# #具体的处理函数，负责处理单个任务
# def do_somthing_using(a):
#     tick = ('%06d' % a[2])
#     st = ts.Market()
#     beg = time.strftime('%Y%m%d',time.strptime(a[10],'%Y-%m-%d'))
#     end = time.strftime('%Y%m%d',time.localtime())
#     print '查询%s从%s到%s的数据' %(tick,beg,end)
#     bGetData = False
#     nLoop = 0
#
#     # #是否已经下载
#     # for h in hasDownload:
#     #     if h[0].find(tick)!=-1:
#     #         bGetData = True
#     #         break
#
#     #循环3次获取数据
#     while nLoop<3 and bGetData==False:  #循环3次
#         df = st.MktEqud(ticker=tick,beginDate=beg,endDate=end)
#         bGetData = df.shape[0]>0
#         if bGetData==False:
#             nLoop = nLoop+1
#             sleep(random.uniform(0.5,1))
#         else:   #获取成功，保存到数据库中
#             df.to_sql( ('MktEqud%s' % tick),engine,if_exists='append' )
#     if bGetData==False:
#         fails.add(tick)
#         print '未获取到%s' % tick
# #这个是工作进程，负责不断从队列取数据并处理
# def working():
#     while True:
#         arguments = q.get()
#         do_somthing_using(arguments)
#         sleep(1.3)
#         q.task_done()
# #把JOBS排入队列
# for a in all:
#     q.put(a)
# #fork NUM个线程等待队列
# for i in range(NUM):
#     t = Thread(target=working)
#     t.setDaemon(True)
#     t.start()
# #等待所有JOBS完成
# q.join()
# print '获取完成'
# print '失败获取%s' % fails

########################################################################