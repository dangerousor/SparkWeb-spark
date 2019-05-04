#!/usr/bin/python
# -*- coding:utf-8 -*-


def err_wrap(func):
    def wrap(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            print("error happend in excuting : " + func.__name__ + ", got :", e)
            return False, "error happend in excuting : " + func.__name__ + ", got :" + str(e)

    return wrap
