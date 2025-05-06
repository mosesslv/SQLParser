# -*- coding:utf-8 -*-
# CREATED BY: bohuai jiang 
# CREATED ON: 2020/3/4 下午1:18
# LAST MODIFIED ON:
# AIM:

import sys

class myLogger(object):

    def __init__(self, filename:str):
        self.terminal = sys.stdout
        self.log = open(filename,"a")

    def write(self, message):
        self.terminal.write(message)
        self.log.write(message)

    def flush(self):
        pass
