# -*- coding:utf-8 -*-
# CREATED BY: bohuai jiang / lvao
# CREATED ON: 2019/10/16 10:39:14
# LAST MODIFIED ON: 2020/05/22
# AIM: compare SQL similarity
from service.lu_parser_graph.LUSQLParser import LuSQLParser
import itertools as it
from typing import List
from service.sql_parser_graph.SQLReader import read_file_as_str
#---- toplogy level ---#

def prime_encoding():
    pass

#--- data size level ---#

#--- utility ---#
def get_prime_list(list_size:int) -> List[int]:
    # referenece : https://hackernoon.com/prime-numbers-using-python-824ff4b3ea19
    primes = []
    for possiblePrime in range(2, list_size+1):
        isPrime = True
        for num in range(2, int(possiblePrime**0.5)+1):
            if possiblePrime % num == 0:
                break
            primes.append(possiblePrime)
    return primes



def PrimesGetter(list_size:int) -> List[int]:
    primes = []
    for num in range(0, list_size + 1):
       # all prime numbers are greater than 1
       if num > 1:
           for i in range(2, num):
               if (num % i) == 0:
                   break
           else:
               primes.append(num)
    return primes

if __name__ == "__main__":
    sql1 = 'select a from tab '
    sql2 = 'select a,b from tab where id > 10 '

    # ps1 = LuSQLParser(sql1)
    # ps2 = LuSQLParser(sql2)
    sql3 = read_file_as_str('./Complex_SQL')
    ps3 = LuSQLParser(sql3)
    ps3.display_elements()
    # print(ps1.get_topology())
    print(ps3.get_topology())
    print(PrimesGetter(20))