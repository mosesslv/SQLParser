# CREATED BY: bohuai jiang 
# CREATED ON: 2019/8/26
# LAST MODIFIED ON:
# AIM: store all error exceptions
# -*- coding:utf-8 -*-
class AIparserError(Exception):
    '''
    parser Error
    '''

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.code = args[0]
        self.message = "SQL parser 错误"


class SQLSyntaxError(Exception):
    '''
    oracle explain missing
    '''

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.code = args[0]
        self.message = "SQL语法错误"


class SQLInfoMiss(Exception):
    '''
    table info incomplete or missing
    '''

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.code = args[0]
        self.message = "SQL信息缺失"


class SQLConflict(Exception):
    '''
    - table columns is key word
    - tab_info & oracle Explain information is inconsistent
    '''

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.code = args[0]
        self.message = "SQL信息冲突"


class SQLHandleError(Exception):
    '''
    other error, outside our list
    '''

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.code = args[0]
        self.message = "AI处理异常"


class DBTypeError(Exception):
    '''
    other error, outside our list
    '''

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.code = args[0]
        self.message = "该sql不是Mysql"


class AnaylsisInfoNOTAccurate(Exception):
    '''
     other error, outside our list
     '''

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.code = args[0]
        self.message = "统计信息不准确"


class KeyWordsAsAlias(Exception):
    '''
      other error, outside our list
      '''

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.code = args[0]
        self.message = "表别名使用关键字"
