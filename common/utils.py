# -*- coding: UTF-8 -*-

import hashlib
import uuid
import inspect
import random
import string
import json
import datetime


def md5_digest(s):
    """
    计算字符串的MD5码
    :param s:
    :return:
    :rtype : str
    """
    return hashlib.md5(s).hexdigest()


def read_file_line(filename):
    """
    按行读取文件, 返回行列表
    :param filename:
    :return:
    """
    file_object = open(filename, 'r')
    try:
        lines = []
        list_of_all_the_lines = file_object.readlines()

        for line in list_of_all_the_lines:
            l = line.replace("\r", "").replace("\n", "")

            if len(l) > 0:
                lines.append(l)
    finally:
        file_object.close()
    return lines


def get_uuid():
    """
    uuid
    :return:
    """
    return str(uuid.uuid1())


def str_is_none_or_empty(var_str):
    """
    string strip and upper
    :param var_str:
    :return:str
    """
    if var_str is None:
        return True
    else:
        if len(var_str) <= 0:
            return True
        else:
            return False


def list_str_item_clear_empty_line_and_duplication_data(obj):
        """
        字符串元素的list object delete empty line
        :param obj:
        :return: list
        """
        assert isinstance(obj, list)
        i = 0
        while i < len(obj):
            if obj[i] is None or len(str(obj[i])) <= 0:
                obj.pop(i)
                i -= 1
            i += 1
        obj_list = list(set(obj))
        return sorted(obj_list)


def get_current_class_methord_name(obj):
    """
    当前的类和方法名
    class_name.methord_name
    :param obj:
    :return: str
    """
    if obj is not None:
        return "{0}.{1}".format(obj.__class__.__name__, inspect.stack()[1][3])
    else:
        return inspect.stack()[1][3]


def get_random_passwd(length):
    """
    生成指定长度的随机密码
    :param length:
    :return: string
    """
    # 随机出数字的个数
    num_of_num = random.randint(1, length-1)
    num_of_letter = length - num_of_num
    # 选中numOfNum个数字
    slc_num = [random.choice(string.digits) for i in range(num_of_num)]
    # 选中numOfLetter个字母
    slc_letter = [random.choice(string.ascii_letters) for i in range(num_of_letter)]
    # 打乱这个组合
    slc_char = slc_num + slc_letter
    random.shuffle(slc_char)
    # 生成密码
    passwd = ''.join([i for i in slc_char])

    # 首字母不能为数字
    try:
        f = int(passwd[0])
        passwd = "a{0}".format(passwd[1:])
    except:
        pass
    # if passwd[0] in [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]:
    #     passwd[0] = "a"
    return passwd


class CJsonEncoder(json.JSONEncoder):
    """
    处理JSON DUMP的datetime问题
    demo: json.dumps(datalist, cls=CJsonEncoder)
    """
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return obj.strftime('%Y-%m-%d %H:%M:%S.%f')
        else:
            return json.JSONEncoder.default(self, obj)
