#-*- coding: utf-8 -*-，
#coding = utf-8
__author__ = 'lhao'

import tushare as ts
from sqlalchemy import create_engine
import MySQLdb

g_engine = None
g_conn = None

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