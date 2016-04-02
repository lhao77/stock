#-*- coding: utf-8 -*-，
#coding = utf-8
__author__ = 'lhao'

import tushare as ts
from sqlalchemy import create_engine
import MySQLdb

g_engine = None
g_conn = None

g_dropSql = 'drop table if exists %s'
g_selectSql = 'select * from %s'
g_selct_datayest_equSql = 'SELECT  secID,ticker,exchangeCD,ListSectorCD,ListSector,transCurrCD,secShortName,secFullName,listStatusCD,listDate,delistDate,equTypeCD,equType,exCountryCD,partyID,totalShares,nonrestFloatShares,nonrestfloatA,officeAddr,primeOperating,endDate,TShEquity FROM datayest_equ'
g_select_datayest_mktadjf = 'SELECT secID,ticker,exchangeCD,secShortName,secShortNameEn,exDivDate,perCashDiv,perShareDivRatio,perShareTransRatio,allotmentRatio,allotmentPrice,adjFactor,accumAdjFactor,endDate FROM datayest_mktadjf'
g_select_idx = 'select * from _idx_'
g_fetch_time = 18
g_mktidxd = 'mktidxd'
g_mktequd = 'mktequd'

g_datayest_mktadjf = 'datayest_mktadjf'
g_update_config = 'UPDATE `_config` SET `last_daysdata_update_date`=\'%s\' WHERE (`last_daysdata_update_date`=\'%s\')'

g_create_table_mktequd = 'CREATE TABLE IF NOT EXISTS `%s` ( \
  `secID` text,\
  `ticker` bigint(20) DEFAULT NULL,\
  `secShortName` text,\
  `exchangeCD` text,\
  `tradeDate` varchar(255) NOT NULL DEFAULT \'\',\
  `preClosePrice` double DEFAULT NULL,\
  `actPreClosePrice` double DEFAULT NULL,\
  `openPrice` double DEFAULT NULL,\
  `highestPrice` double DEFAULT NULL,\
  `lowestPrice` double DEFAULT NULL,\
  `closePrice` double DEFAULT NULL,\
  `turnoverVol` bigint(20) DEFAULT NULL,\
  `turnoverValue` double DEFAULT NULL,\
  `dealAmount` double DEFAULT NULL,\
  `turnoverRate` double DEFAULT NULL,\
  `accumAdjFactor` double DEFAULT NULL,\
  `negMarketValue` double DEFAULT NULL,\
  `marketValue` double DEFAULT NULL,\
  `PE` double DEFAULT NULL,\
  `PE1` double DEFAULT NULL,\
  `PB` double DEFAULT NULL,\
  `isOpen` bigint(20) DEFAULT NULL,\
  PRIMARY KEY (`tradeDate`)\
) ENGINE=InnoDB DEFAULT CHARSET=utf8'

g_create_table_mktidxd = 'CREATE TABLE IF NOT EXISTS `%s` (\
  `indexID` text,\
  `ticker` varchar(20) DEFAULT NULL,\
  `porgFullName` text,\
  `secShortName` text,\
  `exchangeCD` text,\
  `tradeDate` varchar(255) NOT NULL DEFAULT \'\',\
  `preCloseIndex` double DEFAULT NULL,\
  `openIndex` double DEFAULT NULL,\
  `lowestIndex` double DEFAULT NULL,\
  `highestIndex` double DEFAULT NULL,\
  `closeIndex` double DEFAULT NULL,\
  `turnoverVol` double DEFAULT NULL,\
  `turnoverValue` double DEFAULT NULL,\
  `CHG` double DEFAULT NULL,\
  `CHGPct` double DEFAULT NULL,\
  PRIMARY KEY (`tradeDate`)\
) ENGINE=InnoDB DEFAULT CHARSET=utf8'

g_insert_table_mktequd = 'INSERT INTO %s (secID, ticker, secShortName, exchangeCD, tradeDate, \
preClosePrice, actPreClosePrice, openPrice, highestPrice, lowestPrice, closePrice, turnoverVol, \
turnoverValue, dealAmount, turnoverRate, accumAdjFactor, negMarketValue, marketValue, PE, PE1, PB, isOpen) \
VALUES (\'%s\', \'%s\', \'%s\', \'%s\', \'%s\', \'%s\', \'%s\', \'%s\', \'%s\', \'%s\', \'%s\', \
\'%s\', \'%s\', \'%s\', \'%s\', \'%s\', \'%s\', \'%s\', \'%s\', \'%s\', \'%s\', \'%s\')'

g_insert_table_mktidxd = 'INSERT INTO `%s` VALUES (\'%s\', \'%s\', \'%s\', \'%s\', \'%s\', \'%s\', %s, %s, \
 %s, %s, %s, %s, %s, %s, %s)'

g_tuijian_stock_v16_ziming = '_tuijian_stock_v16_ziming'
g_select_actPreClosePrice = 'SELECT actPreClosePrice FROM `mktequd%s` ORDER BY `tradeDate` DESC LIMIT 0, 2'

def initDb():
    global g_engine
    global g_conn
    g_engine = create_engine('mysql://root:@127.0.0.1/tushare?charset=utf8')
    g_conn=MySQLdb.connect(host='localhost',user='root',passwd='',port=3306,charset='UTF8')
    g_conn.select_db('tushare')

def getEngine():
    return g_engine
def getConn():
    return  g_conn
def getCursor():
    return  g_conn.cursor()

def init():
    initDb()
    ts.set_token('1b1ab99dabcb945fc2f2a10941f5de9f762ae6152f7e17ec7faed38401033c49')
