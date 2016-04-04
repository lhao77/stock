__author__ = 'luo'
#-*- coding: utf-8 -*-

class DataException(Exception):
    def __init__(self, expression, message):
        self.expression = expression
        self.message = message

