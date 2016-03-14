#-*- coding: utf-8 -*-，
#coding = utf-8

import datetime
import time

def getTodayYYmmddStr():
    # return datetime.datetime.now().strftime("%Y%m%d")
    return time.strftime('%Y%m%d', time.localtime())

def ConvertDateStr():
    pass