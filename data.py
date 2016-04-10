#-*- coding: utf-8 -*-

import init
import util
import numexpr
import time
import datetime
from time import sleep
import random
import pandas as pd
import numpy
import stock_exception

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
        self.idxd000001 = None #上证指数

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

    #删除除权的表，根据exDivDate删除旧表
    def DropExDivDate(self,lastFetchDateStr):
        df = pd.DataFrame()
        dt = datetime.datetime.strptime(lastFetchDateStr,"%Y%m%d")
        dt = dt + datetime.timedelta(days=1)
        now = datetime.datetime.now()
        while dt.date()<=now.date():
            st = init.ts.Market()
            dateStr = dt.strftime("%Y%m%d")
            ddf = st.MktAdjf(exDivDate=dateStr)
            print '%s:%s' % (dateStr,ddf.shape[0])
            if ddf.shape[0]>0:
                df = df.append(ddf,ignore_index=True)
            dt = dt + datetime.timedelta(days=1)
        # 删除除权的表
        cur = init.getCursor()
        for i in range(0,df.shape[0]):
            tn = '%s%06d' % (init.g_mktequd,df.iloc[i]['ticker'])
            cur.execute(init.g_dropSql % tn)
            print tn
        init.getConn().commit()
        cur.close()

    #初始化数据
    def InitData(self):
        # 初始化config
        df = pd.read_sql( init.g_selectSql % '_config', init.g_conn)
        if df.shape[0]==1:
            self._config = df.iloc[0]
        lastDate = datetime.datetime.strptime(self._config['last_daysdata_update_date'], '%Y%m%d')

        # 下载基本数据
        # self.FetchAllStocksBasicInfo()
        # self.FetchAllStockQfqFactor()
        # self.FetchAllFundBasicInfo()

        #删除除权的表
        self.DropExDivDate(self._config['last_daysdata_update_date'])

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
        today = datetime.datetime.now()
        if today.hour<init.g_fetch_time :
            today = today - datetime.timedelta(days=1)
        if lastDate.date()<today.date():#是否需要下载数据
            #根据上次最后更新日期，开始FetchDayData
            #根据现在时间，看是否下午18点之前还是之后，生成2个日期差的交易日列表，FetchDayData这些交易日
            count = (today.date()-lastDate.date()).days
            while count > 0:
                theDate = (today-datetime.timedelta(days=count-1)).strftime('%Y%m%d')
                self.FetchDayData(theDate)
                self.FetchIdxDaysDate(theDate)
                count = count-1

        #初始化上证指数
        self.idxd000001 = pd.read_sql('select * from mktidxd000001',init.getConn())

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

        # 创建表mktequdXXXXXX
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


    #得到股票价格autoType 0不复权，1前复权，2后复权
    def GetPrice(self,ticker,_time=datetime.datetime.now(),bForward=True,autoType=1):
        if bForward:
            str = init.g_select_price_mktequd_asc % ('%06d' % ticker,_time.strftime('%Y-%m-%d'))
            df = pd.read_sql(str,init.getConn())
            if df.shape[0]==1:
                openPrice = df.iloc[0]['openPrice']
                accumAdjFactor = df.iloc[0]['accumAdjFactor']
                return openPrice*accumAdjFactor if autoType==1 else openPrice
            else:
                raise stock_exception.DataException('Error in GetPrice','%s;Have:%s' % (str,df.shape[0]))
        else:
            str = init.g_select_price_mktequd_desc % ('%06d' % ticker,_time.strftime('%Y-%m-%d'))
            df = pd.read_sql(str,init.getConn())
            if df.shape[0]==1:
                closePrice = df.iloc[0]['closePrice']
                accumAdjFactor = df.iloc[0]['accumAdjFactor']
                return closePrice*accumAdjFactor if autoType==1 else closePrice
            else:
                raise stock_exception.DataException('Error in GetPrice','%s;Have:%s' % (str,df.shape[0]))

    # #得到股票价格（不复权）。。。不考虑盘中交易，bForward Trude表示向前计算（比如20160101不是交易日，那么算到20160104），autoType 0不复权，1前复权，2后复权
    # def GetPrice(self,ticker,_time,bForward=True,autoType=1):
    #     count = 0
    #     price = -1
    #     qfq = 1
    #     while _time.date()<=datetime.datetime.now().date():
    #         # 向前or向后追溯
    #         ntime = _time+datetime.timedelta(days=count) if bForward else datetime.datetime.now()-datetime.timedelta(days=count)
    #         df = pd.read_sql('select openPrice from %s%s where tradeDate=\'%s\'' % (init.g_mktequd,'%06d' % ticker, ntime.strftime('%Y-%m-%d')),init.getConn())
    #         if df.shape[0]==1:
    #             price = df.iloc[0]['openPrice']
    #             qfq = self.GetQfq(ticker,ntime)
    #             break
    #         else:
    #             count = count+1
    #     if autoType==0:
    #         return price
    #     else:
    #         return price*qfq

    #获取前复权因子
    def GetQfq(self,ticker,_time=datetime.datetime.now()):
        str = init.g_select_price_mktequd_desc % ('%06d' % ticker,_time.strftime("%Y-%m-%d"))
        df = pd.read_sql(str,init.getConn())
        qfq = 1
        if df.shape[0]==1:
            qfq = df.iloc[0]['accumAdjFactor']
        return qfq

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

    # 根据exDivDate删除旧表
    def test4(self):
        # df = pd.DataFrame()
        # dt = datetime.datetime.strptime('20160301',"%Y%m%d")
        # now = datetime.datetime.now()
        # while dt.date()<now.date():
        #     st = init.ts.Market()
        #     dateStr = dt.strftime("%Y%m%d")
        #     ddf = st.MktAdjf(exDivDate=dateStr)
        #     print '%s:%s' % (dateStr,ddf.shape[0])
        #     if ddf.shape[0]>0:
        #         df = df.append(ddf,ignore_index=True)
        #     dt = dt + datetime.timedelta(days=1)
        # df.to_sql('_exdivdate',init.getEngine(),if_exists='replace',index=False)

        cur = init.getCursor()
        df = pd.read_sql('select * from _exdivdate',init.getConn())
        for i in range(0,df.shape[0]):
            tn = '%s%06d' % (init.g_mktequd,df.iloc[i]['ticker'])
            cur.execute(init.g_dropSql % tn)
            print tn
        init.getConn().commit()
        cur.close()

    # 测试函数
    def CountTuijianZiming(self):
        # st = init.ts.Market()
        # df = st.TickRTSnapshot(securityID='000001.XSHG')  #没有权限   2016-4-1 19:18:21
        nowDf = init.ts.get_today_all()
        nowDf.to_sql('_today_all',init.getEngine(),if_exists='replace',index=False)
        # nowDf = pd.read_sql('select * from _today_all',init.getConn())
        #遍历表中的股票id，找出实时数据：今天涨幅，累计涨幅，实时价格
        df = pd.read_sql( ('select * from %s' % init.g_tuijian_stock_v16_ziming), init.getConn())
        df['accumPercent'] = pd.Series(None,index=df.index)
        df['todayPercent'] = pd.Series(None,index=df.index)
        for i in range(0,df.shape[0]):
            dd = df.iloc[i]
            try:
                #计算推荐时价格（前复权）
                tuijian_price = 0
                try:
                    tuijian_price = self.GetPrice(int(dd['tuijian_stock_id']),datetime.datetime.strptime(dd['tuijian_time'], "%Y%m%d"))
                except stock_exception.DataException,e:
                    # print "Error %s: %s" % (e.expression, e.message)
                    openPriceDf = nowDf[nowDf.code==dd['tuijian_stock_id']]
                    if openPriceDf.shape[0]==1:
                        tuijian_price = openPriceDf['open']
                    else:
                        raise e
                #查取现在实时价格。
                nowPrice = -1
                nowPriceDf = nowDf[nowDf.code==dd['tuijian_stock_id']]
                if nowPriceDf.shape[0]==1:
                    nowPrice = nowPriceDf['trade']  # *self.GetQfq(dd['tuijian_stock_id'])  # 最新的价格前复权因子肯定是1
                    df.at[i,'todayPercent'] = nowPriceDf['changepercent']   #得到今天涨幅
                    df.at[i,'accumPercent'] = (nowPrice-tuijian_price)/tuijian_price*100    #得到累计涨幅
                else:   #没查到，说明今天停牌了，向前查找一个最近的日期
                    try:
                        nowPrice = self.GetPrice(int(dd['tuijian_stock_id']),bForward=False)
                        df.at[i,'accumPercent'] = (nowPrice-tuijian_price)/tuijian_price*100    #得到累计涨幅
                    except stock_exception.DataException,e:
                        print "Error %s: %s" % (e.expression, e.message)
            except stock_exception.DataException,e:
                print "Error %s: %s" % (e.expression, e.message)

        print df

    # 统计某个区间内股票的涨幅
    def CountChangePercent(self,startDate,endDate):
        # 因为通联数据库表的数据原因（如果改天停牌，openPrice为0，所以把起始时间提前一天去closePrice）
        ddf = self.idxd000001.query('tradeDate<=\'%s\'' % startDate.strftime("%Y-%m-%d"))
        if ddf.shape[0]==0:
            print '起始日期有错误'
            return
        preStartDate = datetime.datetime.strptime(ddf.iloc[-1]['tradeDate'], "%Y-%m-%d")    #上一个交易日日期，用于获取前复权数据
        ddf = self.idxd000001.query('\'%s\'<=tradeDate<=\'%s\'' % (startDate.strftime("%Y-%m-%d"),endDate.strftime("%Y-%m-%d")))
        n = ddf.shape[0]    # 共有n个交易日
        if n==0:
            print '没有交易日'
            return
        # 转换成实际上的其实交易日日期
        trueStartDate = datetime.datetime.strptime(ddf.iloc[0]['tradeDate'], "%Y-%m-%d")
        trueEndDate = datetime.datetime.strptime(ddf.iloc[n-1]['tradeDate'], "%Y-%m-%d")

        df_CountChangePercentN = pd.DataFrame()
        df_CountChangePercentN['ticker'] = pd.Series(dtype=numpy.int64,index=df_CountChangePercentN.index)
        df_CountChangePercentN['secShortName'] = pd.Series(dtype=numpy.object,index=df_CountChangePercentN.index)
        df_CountChangePercentN['percent'] = pd.Series(dtype=numpy.float64,index=df_CountChangePercentN.index)
        df_CountChangePercentN['startPrice'] = pd.Series(dtype=numpy.float64,index=df_CountChangePercentN.index)
        df_CountChangePercentN['endPrice'] = pd.Series(dtype=numpy.float64,index=df_CountChangePercentN.index)
        df_CountChangePercentN['startDate'] = pd.Series(dtype=numpy.object,index=df_CountChangePercentN.index)
        df_CountChangePercentN['endDate'] = pd.Series(dtype=numpy.object,index=df_CountChangePercentN.index)
        count =0
        for i in range(0,self.allStocksBasicInfo.shape[0]):
            stock = self.allStocksBasicInfo.iloc[i]
            stock_mktequd_name = '%s%06d' % (init.g_mktequd,stock['ticker'])
            if stock_mktequd_name in self.allExistTables:
                # if count ==10:
                #     break
                # count = count+1
                if count%100==0:
                    print 'CountChangePercent:%s' % count
                count = count+1
                df_CountChangePercentN.at[i,'ticker'] = stock['ticker']
                df_CountChangePercentN.at[i,'secShortName'] = stock['secShortName']

                str = 'select * from %s where tradeDate>=\'%s\' and tradeDate<=\'%s\' order by tradeDate' % (stock_mktequd_name,trueStartDate.strftime("%Y-%m-%d"),trueEndDate.strftime("%Y-%m-%d"))
                df = pd.read_sql(str,init.getConn())
                nn = df.shape[0]
                if nn>0:
                    # 考虑到新股没有N个交易日的情况，已经覆盖了停牌的情况
                    startPrice = df.iloc[0]['preClosePrice']*self.GetQfq(stock['ticker'],preStartDate)  # if nn>=n else df.iloc[0]['preClosePrice']*1
                    endPrice = df.iloc[nn-1]['closePrice']*df.iloc[nn-1]['accumAdjFactor']
                    percent = (endPrice-startPrice)/startPrice*100 # if startPrice!=0 else -100
                    df_CountChangePercentN.at[i,'startDate'] = df.iloc[0]['tradeDate']
                    df_CountChangePercentN.at[i,'endDate'] = df.iloc[nn-1]['tradeDate']
                    df_CountChangePercentN.at[i,'percent'] = percent
                    df_CountChangePercentN.at[i,'startPrice'] = startPrice
                    df_CountChangePercentN.at[i,'endPrice'] = endPrice
        # 存储到数据库
        df_CountChangePercentN.to_sql( ('_countchangepercent%s_%s' % (startDate.strftime("%Y%m%d"),endDate.strftime("%Y%m%d"))), init.getEngine(), if_exists='replace',index=False)

    # 统计最近N天股票的涨幅。
    def CountChangePercentN(self,n):
        #计算出N天前的交易日日期
        ddf = self.idxd000001.sort_values(by='tradeDate',ascending=False)
        nn = n-1 if ddf.shape[0]>n else -1
        startDate = datetime.datetime.strptime(ddf.iloc[nn]['tradeDate'], "%Y-%m-%d")
        endDate = datetime.datetime.strptime(ddf.iloc[0]['tradeDate'], "%Y-%m-%d")
        self.CountChangePercent(startDate,endDate)

    # 获取股票的流通盘，市值，。。。
    def FetchStockLiutongpanInfo(self):
        self.allStocksBasicInfo[self.allStocksBasicInfo['ticker']==1]['nonrestFloatShares']     #   totalShares  nonrestfloatA  listDate

    def SelectStock(self):
        tongji_qujian_list = [['20150905','20151015'],['20151016','20151027'],['20151028','20151103'],
                         ['20151104','20151125'],['20151126','20151201'],['20151202','20151231'],['20160101','20160201'],
                         ['20160201','20160222'],['20160223','20160229'],['20160301','20160315'],['20160316','20160407']]
        # 统计个期间涨幅最好的200支股票的平均涨幅，涨幅中位数，整个市场的平均涨幅，涨幅中位数
        tongji_qujian = pd.DataFrame()
        tongji_qujian['tongji_name'] = pd.Series(dtype=numpy.object,index=tongji_qujian.index)
        tongji_qujian['idx_percent'] = pd.Series(dtype=numpy.float64,index=tongji_qujian.index)
        tongji_qujian['top_mean'] = pd.Series(dtype=numpy.float64,index=tongji_qujian.index)
        tongji_qujian['top_median'] = pd.Series(dtype=numpy.float64,index=tongji_qujian.index)
        tongji_qujian['all_mean'] = pd.Series(dtype=numpy.float64,index=tongji_qujian.index)
        tongji_qujian['all_median'] = pd.Series(dtype=numpy.float64,index=tongji_qujian.index)
        tongji_qujian['last_top_mean'] = pd.Series(dtype=numpy.float64,index=tongji_qujian.index)
        tongji_qujian['last_top_median'] = pd.Series(dtype=numpy.float64,index=tongji_qujian.index)

        # 做一个累计统计，统计出上一阶段表现最好的股票、上一阶段表现最差的股票在新阶段的数据。
        # 还可以以周/月为单位，统计下数据
        i = 0
        lastQujianTopList = None
        startTopIdx=0
        endTopIdx=10

        for qujian in tongji_qujian_list:
            all_df = pd.read_sql('select * from _countchangepercent%s_%s' % (qujian[0],qujian[1]),init.getConn())

            startDate = datetime.datetime.strptime(qujian[0], "%Y%m%d")
            endDate = datetime.datetime.strptime(qujian[1], "%Y%m%d")
            idx_df = self.idxd000001.query('\'%s\'<=tradeDate<=\'%s\'' % (startDate.strftime("%Y-%m-%d"),endDate.strftime("%Y-%m-%d")))
            tradeDays = idx_df.shape[0]
            #获取新股
            newStocks = self.allStocksBasicInfo.query('listDate>=\'%s\'' % startDate.strftime('%Y%m%d'))
            newStocksList= newStocks['ticker'].values.tolist()
            #获取前200（不包括新股）
            last_top_df = all_df[~all_df['ticker'].isin(newStocksList)].sort_values(by='percent',ascending=False)[startTopIdx:endTopIdx]

            tongji_qujian.at[i,'tongji_name'] = '%s_%s' % (qujian[0],qujian[1])
            tongji_qujian.at[i,'top_mean'] = round(last_top_df.mean()['percent'],2)
            tongji_qujian.at[i,'top_median'] = round(last_top_df.median()['percent'],2)
            tongji_qujian.at[i,'all_mean'] = round(all_df.mean()['percent'],2)
            tongji_qujian.at[i,'all_median'] = round(all_df.median()['percent'],2)
            if tradeDays>0:
                tongji_qujian.at[i,'idx_percent'] = round((idx_df.iloc[-1]['closeIndex']-idx_df.iloc[0]['preCloseIndex'])/idx_df.iloc[0]['preCloseIndex']*100,2)
                #统计lastQujianTop在本次区间的表现
                if lastQujianTopList != None:
                    lastDdf = all_df[all_df['ticker'].isin(lastQujianTopList)].sort_values(by='percent',ascending=False)
                    tongji_qujian.at[i,'last_top_mean'] = round(lastDdf.mean()['percent'],2)
                    tongji_qujian.at[i,'last_top_median'] = round(lastDdf.median()['percent'],2)
            i = i+1
            lastQujianTopList = last_top_df['ticker'].values.tolist()

        # print tongji_qujian
        tongji_qujian.to_sql('_tongji_qujian_%s_%s' % (startTopIdx,endTopIdx),init.getEngine(),if_exists='replace',index=False)

        # #流通盘小于200
        # df20150905_20151015 = pd.read_sql('select * from _countchangepercent20150905_20151015',init.getConn())
        # df20151016_20151027 = pd.read_sql('select * from _countchangepercent20151016_20151027',init.getConn())
        # df20151028_20151103 = pd.read_sql('select * from _countchangepercent20151028_20151103',init.getConn())
        # df20151104_20151125 = pd.read_sql('select * from _countchangepercent20151104_20151125',init.getConn())
        # df20151126_20151201 = pd.read_sql('select * from _countchangepercent20151126_20151201',init.getConn())
        # df20151202_20151231 = pd.read_sql('select * from _countchangepercent20151202_20151231',init.getConn())
        # df20160101_20160201 = pd.read_sql('select * from _countchangepercent20160101_20160201',init.getConn())
        # df20160201_20160222 = pd.read_sql('select * from _countchangepercent20160201_20160222',init.getConn())
        # df20160223_20160229 = pd.read_sql('select * from _countchangepercent20160223_20160229',init.getConn())
        # df20160301_20160315 = pd.read_sql('select * from _countchangepercent20160301_20160315',init.getConn())
        # df20160316_20160407 = pd.read_sql('select * from _countchangepercent20160316_20160407',init.getConn())
        # # df20160323_20160406 = pd.read_sql('select * from _countchangepercent20160323_20160406',init.getConn())
        #
        # newStocks = self.allStocksBasicInfo.query('listDate>=\'2015-09-05\'')
        # newStocksList= newStocks['ticker'].values.tolist()
        # ddf = df20150905_20151015[~df20150905_20151015['ticker'].isin(newStocksList)].sort_values(by='percent',ascending=False)[0:200]
        # ddf = map(int,ddf['ticker'].values.tolist())
        # df20151016_20151027[df20151016_20151027['ticker'].isin(ddf)].sort_values(by='percent',ascending=False)

        # 统计0915-1015第一波涨的最好的，在调整期的表现
        pass

    # 获取指数暴跌时，当天涨得好的票
    # 获取超跌的股票
    # 初始形成多台排列的技术指标，后面怎么发展
    def SelectStock1(self):
        self.idxd000001.query('')


    # 判断代码为ticker的股票是不是在startDate和endDate之间上市的新股
    def IsNewStock(self,ticker,startDate=datetime.datetime.now(),endDate=datetime.datetime.now()):
        self.allStocksBasicInfo[self.allStocksBasicInfo['ticker']==ticker]['listDate']
########################################################################

if __name__ == '__main__':
    init.init()
    data = Data()
    data.InitData()
    data.SelectStock()
#
#     # data.test2()
#     # data.test3()
#
#     # data.CountTuijianZiming()
#     # data.CountChangePercentN(10)
#     data.CountChangePercent(datetime.datetime.strptime('20160101', "%Y%m%d"),datetime.datetime.strptime('20160201', "%Y%m%d"))
#     data.CountChangePercent(datetime.datetime.strptime('20160201', "%Y%m%d"),datetime.datetime.strptime('20160222', "%Y%m%d"))
#     data.CountChangePercent(datetime.datetime.strptime('20160223', "%Y%m%d"),datetime.datetime.strptime('20160229', "%Y%m%d"))
#     data.CountChangePercent(datetime.datetime.strptime('20160301', "%Y%m%d"),datetime.datetime.strptime('20160315', "%Y%m%d"))
#     data.CountChangePercent(datetime.datetime.strptime('20160316', "%Y%m%d"),datetime.datetime.strptime('20160407', "%Y%m%d"))
#     data.CountChangePercent(datetime.datetime.strptime('20150905', "%Y%m%d"),datetime.datetime.strptime('20151231', "%Y%m%d"))
#     data.CountChangePercent(datetime.datetime.strptime('20150905', "%Y%m%d"),datetime.datetime.strptime('20151015', "%Y%m%d"))
#     data.CountChangePercent(datetime.datetime.strptime('20151016', "%Y%m%d"),datetime.datetime.strptime('20151027', "%Y%m%d"))
#     data.CountChangePercent(datetime.datetime.strptime('20151028', "%Y%m%d"),datetime.datetime.strptime('20151103', "%Y%m%d"))
#     data.CountChangePercent(datetime.datetime.strptime('20151104', "%Y%m%d"),datetime.datetime.strptime('20151125', "%Y%m%d"))
#     data.CountChangePercent(datetime.datetime.strptime('20151126', "%Y%m%d"),datetime.datetime.strptime('20151201', "%Y%m%d"))
#     data.CountChangePercent(datetime.datetime.strptime('20151202', "%Y%m%d"),datetime.datetime.strptime('20151231', "%Y%m%d"))

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

# 2016-4-5 23:05:02 关于前复权因子的计算
# 之前是每天重新下载前复权因子表，狠耗时，且计算不便利
# 现在有新的办法，修改tushare的接口，增加exDivDate，每天爬去当天除权信息，找出有修改的股票，然后删除这这个股票的数据，重新下载
# 那么为了不影响当天的数据，每天早上也更新一次数据，晚上还是要更新（具体的明天早上去试试看，是个什么情况）
# 对于现有数据，找出20160301后的除权数据，删除这些表，重新下载

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