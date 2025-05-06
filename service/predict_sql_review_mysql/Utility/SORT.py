# -*- coding:utf-8 -*-
# CREATED BY: bohuai jiang 
# CREATED ON: 2019/9/5
# LAST MODIFIED ON:
# AIM: sort data
from typing import Optional


class TabTree:
    def __init__(self) -> None:
        self.value = None
        self.name = None
        self.right = None
        self.left = None

    def insert(self, name: str, distinct: float) -> None:
        if self.value is None:
            self.name = name
            self.value = distinct
        else:
            if distinct < self.value:
                if self.left is None:
                    v = TabTree()
                    v.name = name
                    v.value = distinct
                    self.left = v
                else:
                    self.left.insert(name, distinct)
            else:
                if self.right is None:
                    v = TabTree()
                    v.name = name
                    v.value = distinct
                    self.right = v
                else:
                    self.right.insert(name, distinct)

    def tree_traverse(self, increase: bool = True, out: list = [], with_value=False) \
            -> Optional[list]:
        if self.value is None:
            return []
        if increase:
            if self.left:
                self.left.tree_traverse(increase, out, with_value)
            if with_value:
                out.append((self.name, self.value))
            else:
                out.append(self.name)
            # print(self.name)
            if self.right:
                self.right.tree_traverse(increase, out, with_value)
            return out
        else:
            if self.right:
                self.right.tree_traverse(increase, out, with_value)
            if with_value:
                out.append((self.name, self.value))
            else:
                out.append(self.name)
            # print(self.name)
            if self.left:
                self.left.tree_traverse(increase, out, with_value)
            return out
