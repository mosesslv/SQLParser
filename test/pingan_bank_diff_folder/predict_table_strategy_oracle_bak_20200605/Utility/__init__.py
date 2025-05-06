# -*- coding:utf-8 -*-
# CREATED BY: bohuai jiang 
# CREATED ON: 
# LAST MODIFIED ON:
# AIM:

from service.predict_table_strategy_oracle.Utility.myLogger import myLogger
import sys


def print_screen_and_file(filename: str):
    import os
    # - remove log before write
    try:
        os.remove(filename)
    except:
        pass
    sys.stdout = myLogger(filename)


def print_screen_only():
    sys.stdout = sys.__stdout__

