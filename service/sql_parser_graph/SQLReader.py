# -*- coding:utf-8 -*-
# CREATED BY: lv ao
# CREATED ON: 2020-05-22
# LAST MODIFIED ON:
# AIM: read sql as string
import os


def read_file_as_str(file_path):
    if not os.path.isfile(file_path):
        raise TypeError(file_path + " does not exist")
    all_the_text = open(file_path).read()
    return all_the_text
