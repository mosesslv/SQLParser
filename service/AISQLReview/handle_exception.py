# -*- coding:utf-8 -*-


class DBConnectException(Exception):
    """
    db init connect exception
    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.message = args[0]
