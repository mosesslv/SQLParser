# -*- coding:utf-8 -*-
# CREATED BY: bohuai jiang 
# CREATED ON: 2019/9/18
# LAST MODIFIED ON:
# AIM: sort key words

from service.sql_parser_graph.units import ParseUnit

KEY_WEIGTH = {'SELECT': 10,
              'INSERT': 10,
              'WHERE': 8,
              'FROM': 9,
              'VALUES': 8,
              'AND': 5,
              ',': 0}


class Keywordstack:
    def __init__(self):
        self.value = None
        self.weight = -float('inf')
        self.length = 0

    def insert(self, value: ParseUnit) -> None:
        self.length += 1
        if value.name.upper() in KEY_WEIGTH.keys():
            weight = KEY_WEIGTH[value.name.upper()]
        else:
            weight = -1
        if weight > self.weight:
            self.value = value
            self.weight = weight

    def pop(self):
        return self.value

    def reset(self):
        self.value = None
        self.weight = -float('inf')
        self.length = 0

    def is_empty(self):
        if self.length > 0:
            return False
        else:
            return True
