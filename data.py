#-*- coding: utf-8 -*-

import init
import util
import numexpr
import time
import datetime
import calendar
from time import sleep
import random
import pandas as pd
import numpy
import requests
from lxml import etree
import stock_exception

# ##############################################################

def log(func):
    def wrapper(*args, **kw):
        print 'Begin %s at %s:' % (func.__name__, datetime.datetime.now())
        return func(*args, **kw)
    return wrapper

################################################################

#数据类
class Data():
    def __init__(self):
        self.allStocksBasicInfo = None  #所有的股票基本信息，从datayest_equ中查取的
        self.allFundBasicInfo = None    #所有基金基本信息
        self.allExistTables = set([])    #所有已经存在的表信息
        self.fails = set([]) #下载失败的股票ticker
        self._config = None  #配置信息
        self.qfqFactor = None #前复权因子
        self.idxd000001 = None #上证指数

    # 获取全部股票基本信息
    @log
    def FetchAllStocksBasicInfo(self):
        eq = init.ts.Equity()
        df = eq.Equ(equTypeCD='A', listStatusCD='L', field='')
        df.to_sql('datayest_equ',init.getEngine(),if_exists='replace',index=False)
        init.getConn().commit()
        self.allStocksBasicInfo = df

    # 获取全部基金基本信息
    @log
    def FetchAllFundBasicInfo(self):
        st = init.ts.Market()
        df = st.MktFund()
        df.to_sql('datayest_fund',init.getEngine(),if_exists='replace',index=False)
        init.getConn().commit()
        self.allFundBasicInfo = df

    #获取股票的日周月均价
    @log
    def FetchDWMJunjia(self):
        pass

    # 获取所有股票的前复权因子
    @log
    def FetchAllStockQfqFactor(self):
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
            init.getConn().commit()
        self.qfqFactor = df

    #获取股票的概念分类
    #获取股票的行业分类（暂时不做 2016-5-10 22:21:20）
    @log
    def FetchGainianGroup(self):
        #基于所有的股票id查取http://stockpage.10jqka.com.cn/300327/，获取到涉及概念，保存到datayest_equ表中
        stockGainian = pd.DataFrame()
        stockGainian['ticker'] = pd.Series(dtype=numpy.int64,index=stockGainian.index)
        stockGainian['name'] = pd.Series(dtype=numpy.object,index=stockGainian.index)
        stockGainian['ref_gainian'] = pd.Series(dtype=numpy.object,index=stockGainian.index)
        # kk = self.allStocksBasicInfo['ticker'].values.tolist()
        gainian = dict()
        i=0
        for idx in range(0,self.allStocksBasicInfo.shape[0]) :
            # if i>2:
            #     break
            # i = i+1
            ticker_str = '%06d' % self.allStocksBasicInfo.iloc[idx]['ticker']
            r = requests.get(init.g_10jqka_url % ticker_str)
            page = etree.HTML(r.content)
            page = etree.HTML(r.text.encode('utf-8'))
            hs = page.xpath(init.g_10jqka_cont_gainian)
            str = ''    #涉及概念
            try:
                i = 0
                for child in hs[0].getchildren() :
                    if child.text == u'涉及概念：':
                        break
                    i = i+1
                if i<len(hs[0].getchildren()):
                    str = hs[0].getchildren()[i+1].attrib.values()[0]
            except Exception as e:
                print e
            stockGainian.at[idx,'ticker'] = self.allStocksBasicInfo.iloc[idx]['ticker']
            stockGainian.at[idx,'name'] = self.allStocksBasicInfo.iloc[idx]['secShortName']
            stockGainian.at[idx,'ref_gainian'] = str
            idx = idx+1
            ref_list = str.split(u'，')
            for ref in ref_list:
                if gainian.has_key(ref)==False:
                    gainian[ref] = set()
                gainian[ref].add(ticker_str)

        #新建一张表，保存所有的概念
        # print gainian
        gainian_df = pd.read_sql(init.g_selectSql % '_gainian', init.getConn())
        idx = 0
        for n,s in gainian.items():
            gainian_df.at[idx,'name'] = n
            gainian_df.at[idx,'stocks'] = ','.join( a for a in s)
            idx = idx+1
        init.getCursor().execute((init.g_dropSql % '_gainian')) #在调用replase前，需要先删除表
        gainian_df.to_sql('_gainian',init.getEngine(),if_exists='replace',index=False)
        init.getConn().commit()

        #保存到_stock_gainian
        stockGainian.to_sql('_stock_gainian',init.getEngine(),if_exists='replace',index=False)
        init.getConn().commit()

        pass

    #删除除权的表，根据exDivDate删除旧表
    @log
    def DropExDivDate(self,lastFetchDate,exdiv_update_hour):
        df = pd.DataFrame()
        # 在除权更新时间前需要更新当天除权信息
        dt = lastFetchDate + (datetime.timedelta(days=1) if lastFetchDate.hour>=exdiv_update_hour else datetime.timedelta(days=0))
        now = datetime.datetime.now()
        while (dt.date()<now.date() or (dt.date()==now.date() and now.hour>=exdiv_update_hour)):
            st = init.ts.Market()
            dateStr = dt.strftime("%Y%m%d")
            ddf = st.MktAdjf(exDivDate=dateStr)
            print 'DropExDivDate  %s:%s' % (dateStr,ddf.shape[0])
            if ddf.shape[0]>0:
                df = df.append(ddf,ignore_index=True)
            dt = dt + datetime.timedelta(days=1)
        # 删除除权的表
        cur = init.getCursor()
        for i in range(0,df.shape[0]):
            tn = '%s%06d' % (init.g_mktequd,df.iloc[i]['ticker'])
            cur.execute(init.g_dropSql % tn)
            print tn
        # 更新config表
        sql = init.g_update_config % ('last_exdiv_update_time',now.strftime('%Y%m%d%H%M'))
        cur.execute(sql)
        init.getConn().commit()

    #初始化数据
    @log
    def InitData(self):
        # 初始化config
        df = pd.read_sql( init.g_selectSql % '_config', init.g_conn)
        if df.shape[0]==1:
            self._config = df.iloc[0]
        last_basic_info_update_time = datetime.datetime.strptime(self._config['last_basic_info_update_time'], '%Y%m%d%H%M')
        basic_info_update_hour = int(self._config['basic_info_update_hour'])
        last_exdiv_update_time = datetime.datetime.strptime(self._config['last_exdiv_update_time'], '%Y%m%d%H%M')
        exdiv_update_hour = int(self._config['exdiv_update_hour'])
        last_fetch_time = datetime.datetime.strptime(self._config['last_fetch_time'], '%Y%m%d%H%M')
        fetch_hour = int(self._config['fetch_hour'])
        today = datetime.datetime.now()

        # 下载基本数据
        today_basic_info_update_time = datetime.datetime.now()
        today_basic_info_update_time = today_basic_info_update_time.replace(hour=basic_info_update_hour,minute=0,second=0,microsecond=0)
        if last_basic_info_update_time<today_basic_info_update_time :
            self.FetchAllStocksBasicInfo()
            # self.FetchAllStockQfqFactor()
            # self.FetchAllFundBasicInfo()  #数据返回有错误，只能等服务端的修复了，暂时先屏蔽
            cur = init.getCursor()
            sql = init.g_update_config % ('last_basic_info_update_time', today.strftime('%Y%m%d%H%M'))
            cur.execute(sql)
            init.getConn().commit()

        #删除除权的表
        self.DropExDivDate(last_exdiv_update_time,exdiv_update_hour)

        # 初始化股票基本信息表
        if self.allStocksBasicInfo is None :
            self.allStocksBasicInfo = pd.read_sql( init.g_selct_datayest_equSql, init.getConn())
        if self.qfqFactor is None :
            self.qfqFactor = pd.read_sql(init.g_select_datayest_mktadjf, init.getConn())
        if self.allFundBasicInfo is None:
            self.allFundBasicInfo = pd.read_sql(init.g_select_datayest_fund, init.getConn())

        #初始化概念 下载时间过长，且更新频率底，每周更新一次差不多了
        # self.FetchGainianGroup()

        # 初始化已存在的表名
        cur = init.getCursor()
        cur.execute('show tables')
        allTmp = cur.fetchall()
        for a in allTmp:
          self.allExistTables.add(a[0])

        # 更新历史日线数据
        self.FetchAllHistoryDayData()
        # 更新指数日线数据
        self.FetchAllIdxDaysData()
        # 更新基金日线数据
        self.FetchAllFundDayData()

        # 需要按日期补充下载数据
        today = datetime.datetime.now()
        if today.hour<init.g_fetch_time :
            today = today - datetime.timedelta(days=1)
        if last_fetch_time.date()<today.date():#是否需要下载数据
            #根据上次最后更新日期，开始FetchDayData
            #根据现在时间，看是否下午18点之前还是之后，生成2个日期差的交易日列表，FetchDayData这些交易日
            count = (today.date()-last_fetch_time.date()).days
            while count > 0:
                theDate = (today-datetime.timedelta(days=count-1)).strftime('%Y%m%d')
                self.FetchDayData(theDate)
                self.FetchIdxDaysDate(theDate)
                count = count-1
        #下载完成后，更新config表
        sql = init.g_update_config % ('last_fetch_time',today.strftime('%Y%m%d%H%M'))
        cur.execute(sql)
        init.getConn().commit()

        #初始化上证指数
        self.idxd000001 = pd.read_sql('select * from mktidxd000001',init.getConn())

    #获取股票tick的历史日线数据,beg不填写的话为第一天上市的日期（%Y%m%d格式），end不填写的话为最新日期
    def FetchHistoryDayData(self,ticker,beg=None,end=None,st=init.ts.Market()):
        if beg==None:
            beg = '19901010'#time.strftime('%Y%m%d', time.strptime(a[10], '%Y-%m-%d')) #因为1989还没有A股
        if end==None:
            end = time.strftime('%Y%m%d', time.localtime())
        print '查询%s从%s到%s的数据' % (ticker, beg, end)
        bGetData = False
        nLoop = 0
        #init.ts.get_hist_data()
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
                # sleep(random.uniform(0.5, 1))
            else:  # 获取成功，保存到数据库中
                df.to_sql(tableName, init.getEngine(), if_exists='append',index=False)
                init.getConn().commit()
                return True
        if bGetData == False:
            self.fails.add(ticker)
            print '未获取到%s' % ticker
        return False

    #获取所有基金的历史日线数据
    def FetchAllFundDayData(self):
        st = init.ts.Market()
        df = pd.read_sql( init.g_select_datayest_fund, init.g_conn)   #指数基本列表
        df = df['ticker'].values.tolist()
        for dd in df:
            fundTableName = ('%s%s' % (init.g_mktfund,dd)).lower()
            # 该指数表不存在才需要完全下载
            if (fundTableName in self.allExistTables) == False:
                try:
                    sql = init.g_create_table_mktfund % fundTableName
                    init.getCursor().execute(sql)
                    init.getConn().commit()
                    dm = st.MktFundd(ticker=dd)
                    dm.to_sql( fundTableName,init.getEngine(),if_exists='append',index=False )
                    init.getConn().commit()
                    print 'FetchAllFundDayData:%s' % fundTableName
                    sleep(0.1)
                except init.MySQLdb.Error,e:
                    print "Mysql Error %d: %s" % (e.args[0], e.args[1])

    #获取所有股票的历史日线数据
    def FetchAllHistoryDayData(self):
        kk = self.allStocksBasicInfo['ticker'].values.tolist()
        for k in kk:
            ticker = '%06d' % k
            bFind = '%s%s' % (init.g_mktequd,ticker) in self.allExistTables
            if bFind==False:
                self.FetchHistoryDayData(ticker)

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
                    # print newSql
                    cur.execute(newSql)
                except init.MySQLdb.Error,e:
                    print "Mysql Error %d: %s" % (e.args[0], e.args[1])
        init.getConn().commit()

    # 获取指数日行情
    def FetchAllIdxDaysData(self):
        st = init.ts.Market()
        df = pd.read_sql( init.g_select_idx, init.g_conn)   #指数基本列表
        df = df['ticker'].values.tolist()
        for dd in df:
            idxTableName = ('%s%s' % (init.g_mktidxd,dd)).lower()
            # 该指数表不存在才需要完全下载
            if (idxTableName in self.allExistTables) == False:
                try:
                    sql = init.g_create_table_mktidxd % idxTableName
                    init.getCursor().execute(sql)
                    init.getConn().commit()
                    dm = st.MktIdxd(ticker=dd)
                    dm.to_sql( idxTableName,init.getEngine(),if_exists='append',index=False )
                    init.getConn().commit()
                    print 'FetchAllIdxDaysData:%s' % idxTableName
                    sleep(0.5)
                except init.MySQLdb.Error,e:
                    print "Mysql Error %d: %s" % (e.args[0], e.args[1])

    # 获取基金日线数据
    def FetchFundDaysDate(self,dateStr):
        st = init.ts.Market()
        df = st.MktFundd(tradeDate = dateStr)
        bGetData = df.shape[0]>0
        if bGetData:
            print '成功获取%s的基金日行情数据' % dateStr
        else:
            print '未获取到%s的基金日行情数据' % dateStr
        #将基金日行情数据插入到各张表中
        if bGetData:
            self.UpdateIdxDaysData(dateStr,df)

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

    # 根据下载的基金日线数据，插入到各张表
    def UpdateFundDaysData(self,dateStr,df):
        cur = init.getCursor()
        for i in range(0,df.shape[0]):    #(df.shape[0]):
            dd = df.iloc[i]
            fundTableName = ('%s%s' % (init.g_mktfund,dd['ticker'])).lower()
            # 该指数表存在    (None if pd.isnull(dd['exchangeCD']) else dd['exchangeCD'])
            if (fundTableName in self.allExistTables):
                try:
                    insertSql = init.g_insert_table_mktfund % \
                                (fundTableName,dd['secID'],dd['ticker'],dd['exchangeCD'],dd['secShortName'],
                                 dd['tradeDate'],dd['preClosePrice'],dd['openPrice'],dd['highestPrice'],
                                 dd['lowestPrice'],dd['closePrice'],dd['CHG'],dd['CHGPct'],dd['turnoverVol'],
                                 dd['turnoverValue'],dd['discount'],dd['discountRatio'],dd['circulationShares'],
                                 dd['accumAdjFactor'])
                    newSql = insertSql.replace('\'nan\'','NULL')
                    newSql = newSql.replace('nan','NULL')
                    # print newSql
                    cur.execute(newSql)
                except init.MySQLdb.Error,e:
                    print "Mysql Error %d: %s" % (e.args[0], e.args[1])
        init.getConn().commit()

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
                    # print newSql
                    cur.execute(newSql)
                except init.MySQLdb.Error,e:
                    print "Mysql Error %d: %s" % (e.args[0], e.args[1])
        init.getConn().commit()

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
            except Exception as e:
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

    # 过滤基金表
    def test5(self):
        df = pd.read_sql('select * from datayest_fund',init.getConn())
        ll = df['ticker'].values.tolist()
        ss =set([])
        for l in ll:
            if ll.count(l)>1:
                ss.add(l)
        print ss
        #删除这些表
        cur = init.getCursor()
        for s in ss:
            sql = init.g_dropSql % ('%s%s' % (init.g_mktfund,s))
            sql1 = 'delete from %s where ticker=\'%s\' limit 1' % ('datayest_fund',s)
            cur.execute(sql)
            cur.execute(sql1)
        init.getConn().commit()

    # 根据exDivDate删除旧表
    def test4(self):
        df = pd.DataFrame()
        dt = datetime.datetime.strptime('20160301',"%Y%m%d")
        now = datetime.datetime.now()
        while dt.date()<now.date():
            st = init.ts.Market()
            dateStr = dt.strftime("%Y%m%d")
            ddf = st.MktAdjf(exDivDate=dateStr)
            print '%s:%s' % (dateStr,ddf.shape[0])
            if ddf.shape[0]>0:
                df = df.append(ddf,ignore_index=True)
            dt = dt + datetime.timedelta(days=1)
        df.to_sql('_exdivdate',init.getEngine(),if_exists='replace',index=False)
        init.getConn().commit()

        cur = init.getCursor()
        # df = pd.read_sql('select * from _exdivdate',init.getConn())
        for i in range(0,df.shape[0]):
            tn = '%s%06d' % (init.g_mktequd,df.iloc[i]['ticker'])
            cur.execute(init.g_dropSql % tn)
            print tn
        init.getConn().commit()

    # 获取股票的流通盘，市值，。。。
    def GetStockLiutongpanInfo(self,ticker):
        dd = self.allStocksBasicInfo[self.allStocksBasicInfo['ticker']==ticker]     #   totalShares  nonrestfloatA  listDate
        return  dd.iloc[0]['totalShares'],dd.iloc[0]['nonrestfloatA']

    # 测试函数
    def CountTuijianZiming(self):
        # st = init.ts.Market()
        # df = st.TickRTSnapshot(securityID='000001.XSHG')  #没有权限   2016-4-1 19:18:21
        nowDf = init.ts.get_today_all()
        nowDf.to_sql('_today_all',init.getEngine(),if_exists='replace',index=False)
        init.getConn().commit()
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

        df.to_sql('_dav_tongji',init.getEngine(), if_exists='replace',index=False)
        init.getConn().commit()

    # 统计某个区间内股票的涨幅
    def CountChangePercent(self,startDate,endDate=datetime.datetime.now(),need_store = True,need_exclude_new_store = False):
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

        stock_gainian = pd.read_sql('select * from _stock_gainian', init.getConn())
        df_CountChangePercentN = pd.DataFrame()
        df_CountChangePercentN['ticker'] = pd.Series(dtype=numpy.int64,index=df_CountChangePercentN.index)
        df_CountChangePercentN['secShortName'] = pd.Series(dtype=numpy.object,index=df_CountChangePercentN.index)
        df_CountChangePercentN['percent'] = pd.Series(dtype=numpy.float64,index=df_CountChangePercentN.index)
        df_CountChangePercentN['startPrice'] = pd.Series(dtype=numpy.float64,index=df_CountChangePercentN.index)
        df_CountChangePercentN['endPrice'] = pd.Series(dtype=numpy.float64,index=df_CountChangePercentN.index)
        df_CountChangePercentN['startDate'] = pd.Series(dtype=numpy.object,index=df_CountChangePercentN.index)
        df_CountChangePercentN['endDate'] = pd.Series(dtype=numpy.object,index=df_CountChangePercentN.index)
        df_CountChangePercentN['gainian'] = pd.Series(dtype=numpy.object,index=df_CountChangePercentN.index)
        df_CountChangePercentN['shizhi'] = pd.Series(dtype=numpy.float64,index=df_CountChangePercentN.index)
        df_CountChangePercentN['liutong'] = pd.Series(dtype=numpy.float64,index=df_CountChangePercentN.index)
        count =0
        kk = self.allStocksBasicInfo.set_index('ticker').T.to_dict()
        i=0
        for ticker,value in kk.iteritems():
            stock_mktequd_name = '%s%06d' % (init.g_mktequd,ticker)
            if (stock_mktequd_name in self.allExistTables) & (self.IsNewStock(ticker,trueStartDate,trueEndDate)==False):
                # if count ==10:
                #     break
                # count = count+1
                if count%100==0:
                    print 'CountChangePercent:%s' % count
                count = count+1
                df_CountChangePercentN.at[i,'ticker'] = ticker
                df_CountChangePercentN.at[i,'secShortName'] = value['secShortName']

                str = 'select * from %s where tradeDate>=\'%s\' and tradeDate<=\'%s\' order by tradeDate' % (stock_mktequd_name,trueStartDate.strftime("%Y-%m-%d"),trueEndDate.strftime("%Y-%m-%d"))
                df = pd.read_sql(str,init.getConn())
                nn = df.shape[0]
                if nn>0:
                    # 考虑到新股没有N个交易日的情况，已经覆盖了停牌的情况
                    startPrice = df.iloc[0]['preClosePrice']*self.GetQfq(ticker,preStartDate)  # if nn>=n else df.iloc[0]['preClosePrice']*1
                    endPrice = df.iloc[nn-1]['closePrice']*df.iloc[nn-1]['accumAdjFactor']
                    percent = (endPrice-startPrice)/startPrice*100 # if startPrice!=0 else -100
                    df_CountChangePercentN.at[i,'startDate'] = df.iloc[0]['tradeDate']
                    df_CountChangePercentN.at[i,'endDate'] = df.iloc[nn-1]['tradeDate']
                    df_CountChangePercentN.at[i,'percent'] = percent
                    df_CountChangePercentN.at[i,'startPrice'] = startPrice
                    df_CountChangePercentN.at[i,'endPrice'] = endPrice
                    df_gn = stock_gainian.query('ticker==%s' % ticker)
                    if df_gn.shape[0]==1:
                        df_CountChangePercentN.at[i,'gainian'] = df_gn.iloc[0]['ref_gainian']
                    df_CountChangePercentN.at[i,'shizhi'] = value['totalShares']*startPrice/100000000
                    df_CountChangePercentN.at[i,'liutong'] = value['nonrestfloatA']*startPrice/100000000
            i = i+1
        # 存储到数据库
        tableName = ('_countchangepercent%s_%s' % (startDate.strftime("%Y%m%d"),endDate.strftime("%Y%m%d")))
        if need_store:
            df_CountChangePercentN.to_sql( tableName, init.getEngine(), if_exists='replace',index=False)
            init.getConn().commit()
        return df_CountChangePercentN,tableName

    # 统计最近N天股票的涨幅。
    def CountChangePercentN(self,n,need_store = True):
        #计算出N天前的交易日日期
        ddf = self.idxd000001.sort_values(by='tradeDate',ascending=False)
        nn = n-1 if ddf.shape[0]>n else -1
        startDate = datetime.datetime.strptime(ddf.iloc[nn]['tradeDate'], "%Y-%m-%d")
        endDate = datetime.datetime.strptime(ddf.iloc[0]['tradeDate'], "%Y-%m-%d")
        return self.CountChangePercent(startDate,endDate,need_store)

    # 按区间来计算股票的平均涨幅，涨幅中位数
    def SelectStock(self,tongji_qujian_list,startTopIdx=0,endTopIdx=10):
        # tongji_qujian_list = [['20150905','20151015'],['20151016','20151027'],['20151028','20151103'],
        #                  ['20151104','20151125'],['20151126','20151201'],['20151202','20151231'],['20160101','20160201'],
        #                  ['20160201','20160222'],['20160223','20160229'],['20160301','20160315'],['20160316','20160407']]

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
        init.getConn().commit()
        # 统计0915-1015第一波涨的最好的，在调整期的表现
        pass

    #对统计区间的数据进行汇总
    def Sum(self,sum_qujian_list):
        # sum_qujian_list = [[0,10],[0,20],[0,50],[0,100],[0,200],[20,50],[50,100],[100,200]]
        # 统计个期间涨幅最好的200支股票的平均涨幅，涨幅中位数，整个市场的平均涨幅，涨幅中位数
        sum_qujian = pd.DataFrame()
        sum_qujian['tongji_name'] = pd.Series(dtype=numpy.object,index=sum_qujian.index)
        sum_qujian['last_top_idx_mean_sum_percent'] = pd.Series(dtype=numpy.float64,index=sum_qujian.index)
        sum_qujian['last_top_idx_mean_success_percent'] = pd.Series(dtype=numpy.float64,index=sum_qujian.index)
        sum_qujian['last_top_all_mean_sum_percent'] = pd.Series(dtype=numpy.float64,index=sum_qujian.index)
        sum_qujian['last_top_all_mean_success_percent'] = pd.Series(dtype=numpy.float64,index=sum_qujian.index)
        sum_qujian['last_top_all_median_sum_percent'] = pd.Series(dtype=numpy.float64,index=sum_qujian.index)
        sum_qujian['last_top_all_median_success_percent'] = pd.Series(dtype=numpy.float64,index=sum_qujian.index)

        i=0
        for qujian in sum_qujian_list:
            all_df = pd.read_sql('select * from _tongji_qujian_%s_%s' % (qujian[0],qujian[1]),init.getConn())
            sum_qujian.at[i,'tongji_name'] = '%s_%s' % (qujian[0],qujian[1])

            last_top_idx_mean_sum_percent = 0.0
            last_top_idx_mean_success_percent = 0.0
            last_top_all_mean_sum_percent = 0.0
            last_top_all_mean_success_percent = 0.0
            last_top_all_median_sum_percent = 0.0
            last_top_all_median_success_percent = 0.0
            for idx in range(0,all_df.shape[0]):
                if pd.isnull(all_df.iloc[idx]['last_top_mean']) == False:
                    last_top_idx_mean_sum_percent += (all_df.iloc[idx]['last_top_mean']-all_df.iloc[idx]['idx_percent'])
                    last_top_idx_mean_success_percent += 1 if (all_df.iloc[idx]['last_top_mean']>all_df.iloc[idx]['idx_percent']) else 0
                    last_top_all_mean_sum_percent += (all_df.iloc[idx]['last_top_mean']-all_df.iloc[idx]['all_mean'])
                    last_top_all_mean_success_percent += 1 if (all_df.iloc[idx]['last_top_mean']>all_df.iloc[idx]['all_mean']) else 0
                    last_top_all_median_sum_percent += (all_df.iloc[idx]['last_top_median']-all_df.iloc[idx]['all_median'])
                    last_top_all_median_success_percent += 1 if (all_df.iloc[idx]['last_top_median']>all_df.iloc[idx]['all_median']) else 0
            num = all_df.shape[0]-1 if (all_df.shape[0]-1)!=0 else 1
            last_top_idx_mean_success_percent = round(last_top_idx_mean_success_percent/num,2)
            last_top_all_mean_success_percent = round(last_top_all_mean_success_percent/num,2)
            last_top_all_median_success_percent = round(last_top_all_median_success_percent/num,2)

            sum_qujian.at[i,'last_top_idx_mean_sum_percent'] = last_top_idx_mean_sum_percent
            sum_qujian.at[i,'last_top_idx_mean_success_percent'] = last_top_idx_mean_success_percent
            sum_qujian.at[i,'last_top_all_mean_sum_percent'] = last_top_all_mean_sum_percent
            sum_qujian.at[i,'last_top_all_mean_success_percent'] = last_top_all_mean_success_percent
            sum_qujian.at[i,'last_top_all_median_sum_percent'] = last_top_all_median_sum_percent
            sum_qujian.at[i,'last_top_all_median_success_percent'] = last_top_all_median_success_percent
            i = i+1

        sum_qujian.to_sql('_sum_qujian',init.getEngine(),if_exists='replace',index=False)
        init.getConn().commit()

    # 给2个区间，找出能在两个区间都排在前列的股票
    def SelectQiangStock(self,tongji_qujian_list,top=200):
        all_df = list()
        for qujian in tongji_qujian_list:
            sql = 'select * from _countchangepercent%s_%s WHERE startDate = \'%s\' ORDER BY percent DESC LIMIT 0, %s' \
                  % (qujian[0],qujian[1],datetime.datetime.strptime(qujian[0], "%Y%m%d").strftime("%Y-%m-%d"),top)
            df = pd.read_sql(sql,init.getConn())
            all_df.append(df)

        select_tickers = list()
        size = len(all_df)
        kk = self.allStocksBasicInfo['ticker'].values.tolist()
        for k in kk:
            count = 0
            idx = 0
            for df in all_df:
                dd = df.query(('ticker==%s' % k))
                if dd.shape[0]==1:
                    count = count+1
            if count==size:
                select_tickers.append(k)
        print select_tickers



    # 获取指数暴跌时，当天涨得好的票
    # 获取超跌的股票
    # 初始形成多台排列的技术指标，后面怎么发展
    def SelectStock1(self):
        self.idxd000001.query('')

    # 统计20150815以来按周的统计数据
    def SumByWeek(self):
                # # 统计时间区间、股票排名区间
        # tongji_qujian_list = [['20150905','20151015'],['20151016','20151027'],['20151028','20151103'],
        #                      ['20151104','20151125'],['20151126','20151201'],['20151202','20151231'],['20160101','20160201'],
        #                      ['20160201','20160222'],['20160223','20160229'],['20160301','20160315'],['20160316','20160407']]
        # sum_qujian_list = [[0,10],[0,20],[0,50],[0,100],[0,200],[20,50],[50,100],[100,200]]
        # # for sl in sum_qujian_list:
        # #     data.SelectStock(tongji_qujian_list,sl[0],sl[1])
        # data.Sum(sum_qujian_list)
        #得出的结论是强者恒强，
    #     将9.15以来的行情按照指数涨、调整、跌分为11个阶段。统计每个阶段表现好的股票在下个阶段的表现。
    # [['20150905','20151015'],['20151016','20151027'],['20151028','20151103'],['20151104','20151125'],['20151126','20151201'],['20151202','20151231'],['20160101','20160201'],['20160201','20160222'],['20160223','20160229'],['20160301','20160315'],['20160316','20160407']]
    # tongji_name表示阶段排名 0——10 指排名前十的，0-20表示排名0-20的
    # 以排名0-10的为例。每个阶段你都买入前十的股票，直到下个阶段卖出，可以超越大盘136.79%，操作成功率是90%。
    # 超越所有股票平均涨幅达到109.86%。操作成功率是80%。超越所有股票涨幅中位数59.49%，操作成功率是70%。

        #以周为单位进行统计。选定一个起始日期，从idx000001中找出以周为单位的区间，生成统计区间
        startDate = datetime.datetime(2016,4,18)# datetime.datetime(2015,8,15)
        tongji_qujian_list = list()
        jy_date_list = data.idxd000001.query('tradeDate>=\'%s\'' % startDate.strftime('%Y-%m-%d'))
        jy_date_list = jy_date_list['tradeDate'].values.tolist()
        startDate = datetime.datetime.strptime(jy_date_list[0],"%Y-%m-%d")
        de = (calendar.MONDAY-startDate.weekday()) % 7
        next_Monday = startDate + datetime.timedelta(days=de if de!=0 else 7)

        i=0
        for jy in jy_date_list:
            jyDate = datetime.datetime.strptime(jy,"%Y-%m-%d")
            if jyDate>=next_Monday:
                tongji_qujian_list.append([startDate.strftime('%Y%m%d'),jy_date_list[i-1].replace('-','')])
                startDate = jyDate
                de = (calendar.MONDAY-startDate.weekday()) % 7
                next_Monday = startDate + datetime.timedelta(days=de if de!=0 else 7)
            if next_Monday>= datetime.datetime.now():
                tongji_qujian_list.append([startDate.strftime('%Y%m%d'),jy_date_list[-1].replace('-','')])
                break
            i = i+1
        print tongji_qujian_list
        # 对区间进行统计
        for tj in tongji_qujian_list:
            data.CountChangePercent(datetime.datetime.strptime(tj[0], "%Y%m%d"),datetime.datetime.strptime(tj[1], "%Y%m%d"))
        sum_qujian_list = [[0,10],[0,20],[0,50],[0,100],[0,200],[20,50],[50,100],[100,200]]
        for sl in sum_qujian_list:
            data.SelectStock(tongji_qujian_list,sl[0],sl[1])
        data.Sum(sum_qujian_list)

    # 有没有上一波类似现在的情况的？分析下
    # 在大盘3050时的股价，今天到了3050-3100之间，现在的股价，    2016-4-14 10:05:23
    # 滞胀的，小盘
    def SelectStock3050(self):
        sd=datetime.datetime.strptime('20160321','%Y%m%d')
        df = self.CountChangePercent(startDate=sd,need_store=False)
        # 过滤掉大市值的
        guolv = set([])
        for i in range(0,df.shape[0]):
            totalShares,nonrestfloatA = self.GetStockLiutongpanInfo(df.iloc[i]['ticker'])
            total = totalShares*df.iloc[i]['startPrice']/100000000.0
            floatA = nonrestfloatA*df.iloc[i]['startPrice']/100000000.0
            if total>200 or floatA>150:
                guolv.add(df.iloc[i]['ticker'])
        #
        df.set_index('ticker')
        for g in guolv:
            df = df.drop(df.index['ticker'==g])
        df.to_sql('_select_stock3050',init.getEngine(),if_exists='replace',index=False)
        init.getConn().commit()

    #统计暴跌当天表现好的股票第二天的涨幅

    # 判断代码为ticker的股票是不是在startDate和endDate之间上市的新股
    def IsNewStock(self,ticker,startDate=datetime.datetime.now(),endDate=datetime.datetime.now()):
        listDate = self.allStocksBasicInfo[self.allStocksBasicInfo['ticker']==ticker]
        if listDate.shape[0]==1:
            listDate = listDate.iloc[0]['listDate']
            listDate = datetime.datetime.strptime(listDate, "%Y-%m-%d")
            return endDate>=listDate>=startDate
        return False

    '''
    很多票在主升过程中有回踩动作，研究下，他们在20，30，45,60,120等日线的支撑位后的走势。
    '''
    #统计CountChangePercent中市值、流通市值分布，板块分布
    def SumCountChangePercent(self,tableName,N,bAsc):
        #市值按
        def getQujianString(idx,li):
            length = len(li)
            if idx<length-1:
                return '%05d-%05d' % (li[idx],li[idx+1])
            elif idx==length-1:
                return '%05d-' % li[idx]
            else:
                return 'error'

        shizhi_qujian = [0,50,100,200,300,500,1000]
        liutong_qujian = map(lambda x:x/2,shizhi_qujian)
        sz_dict = dict()
        lt_dict = dict()
        sql = 'select * from %s order by percent %s limit 0,%s' % (tableName, 'asc' if bAsc==True else 'desc', N)
        df = pd.read_sql(sql,init.getConn())
        for i in range(0,df.shape[0]):
            idx = 0
            while idx < len(shizhi_qujian):
                if df.iloc[i]['shizhi']>=shizhi_qujian[idx]:
                    idx = idx+1
                else:
                    break
            str = getQujianString(idx-1,shizhi_qujian)
            if sz_dict.has_key(str)==False:
                sz_dict[str] = set([])
            sz_dict[str].add(df.iloc[i]['ticker'])

            idx = 0
            while idx < len(liutong_qujian):
                if df.iloc[i]['liutong']>liutong_qujian[idx]:
                    idx = idx+1
                else:
                    break
            str = getQujianString(idx-1,liutong_qujian)
            if lt_dict.has_key(str)==False:
                lt_dict[str] = set([])
            lt_dict[str].add(df.iloc[i]['ticker'])
        # for k,v in sz_dict.iteritems():
        #     print k,v
        # for k,v in lt_dict.iteritems():
        #     print k,v
        print sz_dict
        print lt_dict

        # shizhi_qujian = [0,50,100,200,300,500,1000]
        # sz_list = list()
        # sz_str_list = list()
        # for i in range(0,len(shizhi_qujian)):
        #     if i<len(shizhi_qujian)-1:
        #         sz_str_list.append('%s-%s' % (shizhi_qujian[i],shizhi_qujian[i+1]))
        #         sz_list.append(set([]))
        #     else:
        #         sz_str_list.append('%s-' % (shizhi_qujian[i]))
        #         sz_list.append(set([]))
        # liutong_qujian = map(lambda x:x/2,shizhi_qujian)
        # lt_list = list()
        # lt_str_list = list()
        # for i in range(0,len(liutong_qujian)):
        #     if i<len(liutong_qujian)-1:
        #         lt_str_list.append('%s-%s' % (liutong_qujian[i],liutong_qujian[i+1]))
        #         lt_list.append(set([]))
        #     else:
        #         lt_str_list.append('%s-' % (liutong_qujian[i]))
        #         lt_list.append(set([]))

        #
        # sql = 'select * from %s order by percent %s limit 0,%s' % (tableName, 'asc' if bAsc==True else 'desc', N)
        # df = pd.read_sql(sql,init.getConn())
        # for i in range(0,df.shape[0]):
        #     idx = 0
        #     while idx < len(shizhi_qujian):
        #         if df.iloc[i]['shizhi']>=shizhi_qujian[idx]:
        #             idx = idx+1
        #         else:
        #             break
        #     sz_list[idx-1].add(df.iloc[i]['ticker'])
        #
        #     idx = 0
        #     while idx < len(liutong_qujian):
        #         if df.iloc[i]['liutong']>liutong_qujian[idx]:
        #             idx = idx+1
        #         else:
        #             break
        #     lt_list[idx-1].add(df.iloc[i]['ticker'])
        #
        # for i in range(0,len(sz_list)):
        #     print sz_str_list[i],len(sz_list[i])
        # for i in range(0,len(lt_list)):
        #     print lt_str_list[i],len(lt_list[i])

########################################################################

if __name__ == '__main__':
    print '开始时间:%s' % datetime.datetime.now()
    init.init()
    data = Data()
    data.InitData()

    # data.CountChangePercent(datetime.datetime.strptime('20160509', "%Y%m%d"),datetime.datetime.strptime('20160512', "%Y%m%d"))
    # data.SumCountChangePercent('_countchangepercent20160509_20160512',200,False)
    # data.CountChangePercent(datetime.datetime.strptime('20160315', "%Y%m%d"))
    # qujian=[['20150817','20160426'],['20160104','20160426']]

    # data.SelectQiangStock(qujian)
    # data.CountChangePercent(datetime.datetime.strptime('20150817', "%Y%m%d"))
    # data.CountChangePercent(datetime.datetime.strptime('20160104', "%Y%m%d"))
    # data.SelectStock3050()
    # data.SumByWeek()

#
#     # data.test2()
#     # data.test3()
#     data.test4()

    # data.CountTuijianZiming()
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

    init.getCursor().close()
    init.getConn().close()
    print '结束时间:%s' % datetime.datetime.now()

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