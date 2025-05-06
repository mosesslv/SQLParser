# -*- coding:utf-8 -*-
# CREATED BY: bohuai jiang 
# CREATED ON: 2019/12/30 上午10:09
# LAST MODIFIED ON:
# AIM:

from typing import List, Dict
import numpy as np


class Encoding:

    def __init__(self):
        pass

    def encode(self, *args):
        pass

    def decode(self, *args):
        pass


class PrimeEncoding(Encoding):

    def __init__(self):
        super().__init__()
        self.decode_map = dict()
        try:
            self.code_map = np.load('prime_encode.npy', allow_pickle=True).item()
            assert isinstance(self.code_map, dict)
            for key, value in self.code_map.items():
                self.decode_map[value] = key
        except:
            self.code_map = dict()

    def _get_prime(self, pre_prime: int) -> int:
        num = pre_prime + 1
        while True:
            for i in range(2, num):
                if (num % i) == 0:
                    break
            else:
                return num
            num += 1

    def _find_dividend(self, divisor: int) -> int:
        if len(self.code_map) < 1:
            raise CodeMapNotFound('找不到code map, 需要code map才能进行编码, 请调用build_code_map()')
        for dividend in self.code_map.values():
            mode = divisor % dividend
            if mode == 0:
                return dividend
        return divisor

    def build_code_map(self, title_list: List[str], save_to_npy=False) -> Dict[str, int]:
        '''

        :param title_list: ['a', 'b', 'c']
        :return: {'a': 2.0, 'b': 3.0, 'c': 5.0}
        '''
        self.code_map = {title_list[0]: 2}
        self.decode_map = {2 :title_list[0]}
        pre_prime = 2
        for w in title_list[1::]:
            pre_prime = self._get_prime(pre_prime)
            self.code_map[w] = pre_prime
            self.decode_map[pre_prime] = w
        if save_to_npy:
            np.save('prime_encode.npy', self.code_map, allow_pickle=True)
        return self.code_map

    def encode(self, comb_words: List[str]) -> int:
        '''

        :param comb_words: ['a','b'.'c']
        :return: 2*3*5 = 30
        '''
        if len(self.code_map) < 1:
            raise CodeMapNotFound('找不到code map, 需要code map才能进行编码, 请调用build_code_map()')

        out = 1
        for v in comb_words:
            try:
                out = out * self.code_map[v]
            except:
                raise CodeMapNotIncomplete('code map 不完整,　需要重新生成code map,　请调用build_code_map()')
        return out

    def decode(self, code: int) -> List[str]:
        '''

        :param code: 24
        :return: 2*2*2*3 = ['a', 'a', 'a', 'b']
        '''
        max_itr = 1024
        cnt_itr = 0
        out = []
        while True:
            dividend = self._find_dividend(code)
            try:
                out.append(self.decode_map[dividend])
            except:
                raise CodeMapNotIncomplete('code map 不完整,　需要重新生成code map,　请调用build_code_map()')
            if code == dividend:
                break
            else:
                code = code/dividend
            cnt_itr += 1
            if cnt_itr > max_itr:
                raise ExceedMaxIteration(f'算法错误，超出最大循环次数{max_itr:d}')
        return out

class LogPrimeEncoding(PrimeEncoding):

    def __init__(self):
        super().__init__()
        self.decode_map = dict()
        try:
            self.code_map = np.load('prime_encode.npy', allow_pickle=True).item()
            assert isinstance(self.code_map, dict)
            for key, value in self.code_map.items():
                self.decode_map[value] = key
        except:
            self.code_map = dict()

    def encode(self, comb_words: List[str]) -> int:
        '''

        :param comb_words: ['a','b'.'c']
        :return: 2*3*5 = 30
        '''
        if len(self.code_map) < 1:
            raise CodeMapNotFound('找不到code map, 需要code map才能进行编码, 请调用build_code_map()')

        out = 0
        for v in comb_words:
            try:
                out = out + np.log(float(self.code_map[v]))
            except:
                raise CodeMapNotIncomplete('code map 不完整,　需要重新生成code map,　请调用build_code_map()')
        return out

    def decode(self, code: float) -> List[str]:
        '''

        :param code: 24
        :return: 2*2*2*3 = ['a', 'a', 'a', 'b']
        '''
        max_itr = 1024
        cnt_itr = 0
        out = []
        code = np.round(np.exp(code))
        while True:
            dividend = self._find_dividend(code)
            try:
                out.append(self.decode_map[dividend])
            except:
                raise CodeMapNotIncomplete('code map 不完整,　需要重新生成code map,　请调用build_code_map()')
            if code == dividend:
                break
            else:
                code = code / dividend
            cnt_itr += 1
            if cnt_itr > max_itr:
                raise ExceedMaxIteration(f'算法错误，超出最大循环次数{max_itr:d}')
        return out


# --------------- #
#   Exceptions    #
# --------------- #
class CodeMapNotFound(Exception):
    pass


class CodeMapNotIncomplete(Exception):
    pass

class ExceedMaxIteration(Exception):
    pass

if __name__ == '__main__':
    test = PrimeEncoding()
    v = test.build_code_map('a b c d e f g h i j k l m n o p q r s t u v w x y z'.split())
    print(test.encode('z z z z z z z z z z z z z z z z z z z z z z z z z z z z z z z z z z z z z z z z '.split()))
    #print(test.decode(13.8454))