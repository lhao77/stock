#-*- coding: utf-8 -*-，
#coding = utf-8
import init
import util
import numexpr
import time
from time import sleep
import random
import pandas as pd
# ##############################################################

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

# 获取所有股票的前复权因子
# st = ts.Market()
# maxids = 50
# idx = 0
# l = len(all)
# while(idx<l):
#     str_ticker = ','.join( ('%06d' % a[0]) for a in all[idx:idx+maxids] )
#     idx = idx+maxids
    # df = st.MktAdjf(ticker=str_ticker)
    # df.to_sql('datayest_MktAdjf',engine,if_exists='append')
    # df = st.MktAdjfAf(ticker=str_ticker)
    # df.to_sql('datayest_MktAdjfAf',engine,if_exists='append')
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
#数据类
class Data():
    def __init__(self):
        self.allStocksBasicInfo = None  #所有的股票基本信息，从datayest_equ中查取的
        self.allExistTables = list()    #所有已经存在的表信息
        self.remainAll = list()    #剩余待查取的股票代码list
        # self.InitData()
        # self.hasDownload = None  #已经下载的股票ticker，
        self.fails = set([]) #下载失败的股票ticker

    # 获取全部股票基本信息
    def FetchAllStocksBasicInfo(self):
        eq = init.ts.Equity()
        df = eq.Equ(equTypeCD='A', listStatusCD='L', field='')
        df.to_sql('datayest_equ',init.getEngine(),if_exists='replace')

    #初始化数据
    def InitData(self):
        self.FetchAllStocksBasicInfo()
        cur = init.getCursor()
        cur.execute(r' select * from datayest_equ')
        self.allStocksBasicInfo = cur.fetchall()
        # print '共有 %s支股票' % n

        cur.execute('show tables')
        allTmp = cur.fetchall()
        for a in allTmp:
          self.allExistTables.append(a[0])
        # print '已经爬取了%s支' % n  #不是正确的，但不影响结果，所以暂时不处理

        # 过滤已经爬取的数据
        for a in self.allStocksBasicInfo:
            tick = ('%06d' % a[2])
            bn = False
            for h in self.allExistTables:
                if h.find(tick) != -1:
                    bn = True
                    break
            if bn == False:
                self.remainAll.append(a)

    #根据表名判断表是否存在
    def IsTableExist(self,name):
        for a in self.allExistTables:
            if a.find(name)!=-1 :
                return True
        return False

    #获取股票tick的历史日线数据,beg不填写的话为第一天上市的日期（%Y%m%d格式），end不填写的话为最新日期
    def FetchHistoryDayData(self,ticker,beg=None,end=None,st=init.ts.Market()):
        if beg==None:
            beg = '19891212'#time.strftime('%Y%m%d', time.strptime(a[10], '%Y-%m-%d')) #因为1989还没有A股
        if end==None:
            end = time.strftime('%Y%m%d', time.localtime())
        print '查询%s从%s到%s的数据' % (ticker, beg, end)
        bGetData = False
        nLoop = 0

        # 循环3次获取数据
        while nLoop < 3 and bGetData == False:  # 循环3次
            df = st.MktEqud(ticker=ticker, beginDate=beg, endDate=end)
            bGetData = df.shape[0] > 0
            if bGetData == False:
                nLoop = nLoop + 1
                sleep(random.uniform(0.5, 1))
            else:  # 获取成功，保存到数据库中
                tableName = ('MktEqud%s' % ticker)
                df.to_sql(tableName, init.getEngine(), if_exists='append')
                self.AddPrimaryKey(tableName)
                return True
        if bGetData == False:
            fails.add(ticker)
            print '未获取到%s' % ticker
        return False

    #获取所有股票的历史日线数据
    def FetchAllHistoryDayData(self):
        st = init.ts.Market()
        df = None
        n = 1
        for a in self.remainAll:
            tick = ('%06d' % a[2])
            if self.FetchHistoryDayData(tick) :
                n = n+1
            if n % 50 == 0:
                sleep(random.uniform(3, 5))

    #获取今日行情数据
    def FetchTodayDayData(self):
        todayStr = util.getTodayYYmmddStr()
        self.FetchDayData(todayStr)

    #获取指定日期的日行情数据
    def FetchDayData(self,dateStr):
        st = init.ts.Market()
        df = st.MktEqud(tradeDate=dateStr)
        bGetData = df.shape[0]>0
        if bGetData:
            print '成功获取%s的日行情数据' % dateStr
            df.to_sql( ('%smktequd' % dateStr), init.g_engine)
        else:
            print '未获取到%s的日行情数据' % dateStr
        #将今日行情数据插入到各张表中
        if bGetData:
            pass

    #更新日行情数据
    def UpdateDaysData(self,tradeDate):
        theDate = ('%smktequd' % tradeDate)
        df = pd.read_sql( ('select * from %smktequd' % tradeDate),init.g_conn)
        print df.shape
        cur = init.getCursor()
        conn = init.getConn()
        # cur.execute("SET NAMES utf8") #or utf8 or any other charset you want to handle
        # cur.execute("SET CHARACTER SET utf8") #same as above
        # cur.execute("SET character_set_connection=utf8") #same as above

        for i in range(0,df.shape[0]):    #(df.shape[0]):
            dd = df.iloc[i] #获取一行
            tableName = 'mktequd%06d' % dd[2]
            #判断表是否存在
            bExist = self.IsTableExist(tableName)
            if bExist==True :
                n = cur.execute('select `index` from %s' % tableName)   #获取index值   dd[3].decode('utf-8')
                insertSql = 'INSERT INTO %s (`index`, secID, ticker, secShortName, exchangeCD, tradeDate, preClosePrice, actPreClosePrice, openPrice, highestPrice, lowestPrice, closePrice, turnoverVol, turnoverValue, dealAmount, turnoverRate, accumAdjFactor, negMarketValue, marketValue, PE, PE1, PB, isOpen) VALUES (\'%s\', \'%s\', \'%s\', \'%s\', \'%s\', \'%s\', \'%s\', \'%s\', \'%s\', \'%s\', \'%s\', \'%s\', \'%s\', \'%s\', \'%s\', \'%s\', \'%s\', \'%s\', \'%s\', \'%s\', \'%s\', \'%s\', \'%s\')' % (tableName,n,dd[1],dd[2],dd[3],dd[4],dd[5],dd[6],dd[7],dd[8],dd[9],dd[10],dd[11],dd[12],dd[13],dd[14],dd[15],dd[16],dd[17],dd[18],dd[19],dd[20],dd[21],dd[22])
                print insertSql
                cur.execute(insertSql)
        conn.commit()

    #更改所有股票日行情表的，设置日期主键
# ALTER TABLE `mktequd000001`
# MODIFY COLUMN `tradeDate`  varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' AFTER `exchangeCD`,
# ADD PRIMARY KEY (`tradeDate`);
    def AddPrimaryKey(self,tableName):
        cur = init.getCursor()
        # conn = init.getConn()
        altSql = ('ALTER TABLE `%s` MODIFY COLUMN `tradeDate`  varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT \'\' AFTER `exchangeCD`, ADD PRIMARY KEY (`tradeDate`)' % tableName)
        print altSql
        cur.execute(altSql)

    def AddPrimaryKeyForDaysData(self):
        cur = init.getCursor()
        conn = init.getConn()
        for a in self.allExistTables:
            for b in self.allStocksBasicInfo:
                if a.find( ('%06d' % b[2]) )!=-1:
                    #执行更新命令
                    altSql = ('ALTER TABLE `%s` MODIFY COLUMN `tradeDate`  varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT \'\' AFTER `exchangeCD`, ADD PRIMARY KEY (`tradeDate`)' % a)
                    print altSql
                    cur.execute(altSql)
                    break
        #
        # cur.execute('ALTER TABLE `mktequd000001` MODIFY COLUMN `tradeDate`  varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT \'\' AFTER `exchangeCD`, ADD PRIMARY KEY (`tradeDate`)')

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
                sql = 'select * from %s where tradeDate=\'%s\'' %(('mktequd%s' % dd['tuijian_stock_id']),time.strftime('%Y-%m-%d', time.strptime(dd['tuijian_time'], '%Y%m%d')))
                print sql
                n=cur.execute(sql)
                if n==1:
                    the = cur.fetchall()
                    the[0]
                pass
            if dd['percent_2day']==None :
                pass
            if dd['percent_3day']==None :
                pass

    #Test 找出存在的表，但是在datayest_equ中不存在
    def test(self):
        bb = list()
        for a in self.allExistTables:
            bFind = False
            for b in self.allStocksBasicInfo:
                if a.find( ('%06d' % b[2]) )!=-1:
                    bFind = True
                    break
            if bFind==False:
                bb.append(a)
        print(bb)

    # 找出在datayest_equ中存在但是未保存下来的表
    def test1(self):
        bb = list()
        for b in self.allStocksBasicInfo:
            bFind = False
            for a in self.allExistTables:
                if a.find( ('%06d' % b[2]) )!=-1:
                    bFind = True
                    break
            if bFind==False:
                bb.append(a)
        print bb

########################################################################

if __name__ == '__main__':
    init.init()
    data = Data()
    data.InitData()

    data.FetchDayData('20160311')
    data.UpdateDaysData('20160311')

    # data.FetchAllHistoryDayData()
    # data.FetchTodayDayData()
    # data.UpdateDaysData(util.getTodayYYmmddStr())

    # data.CountTuijianStockData()

    # print data.remainAll
    # data.FetchHistoryDayData('603999','20151210')
    # data.FetchAllHistoryDayData()
    # data.FetchDayData('20160311')
    # data.UpdateDaysData('20160311')
    # data.test()
    # data.test1()
    # data.AddPrimaryKeyForDaysData()

    # 日行情的处理流程：
    # 1，重新更新datayest_equ
    # 2. FetchAllHistoryDayData会会新股票创建出新表mktequdXXXXXX，并会调用AddPrimaryKey处理好表结构
    # 3. FetchTodayDayData获取到今日行情
    # 4. UpdateDaysData更新今日行情到各张mktequdXXXXXX中