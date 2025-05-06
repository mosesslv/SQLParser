# -*- coding:utf-8 -*-
# title = ''
# CREATED BY: 'lvao513'
# CREATED ON: '2020/6/15'
# LAST MODIFIED ON: '2020/6/15'
# GOAL: ......


class Foo(object):
    def x(self):
        print('Foo')


# class Foo2(Foo):
#     def x(self):
#         print('Foo2')
#         super(self.__class__, self).x()  # wrong


class Foo3(Foo):
    def x(self):
        print('Foo3')
        super(Foo3, self).x()


f = Foo3()
f.x()

