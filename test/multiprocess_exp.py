# -*- coding:utf-8 -*-
# CREATED BY: bohuai jiang 
# CREATED ON: 
# LAST MODIFIED ON:
# AIM:
from multiprocessing import Pool
import time
from functools import partial


def mycallback(x, l):
    #l.append(x)
    try:
        l['a'].append(x)
    except:
        l['a']= [x]

def sayHi(num):
  return num


if __name__ == '__main__':
    e1 = time.time()
    pool = Pool()
    l = dict()
    for i in range(10):
        pool.apply_async(sayHi, (i,), callback=partial(mycallback,l =l))

    pool.close()
    pool.join()
    print(l)