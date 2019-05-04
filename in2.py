#!/usr/bin/python
# -*- coding:utf-8 -*-
from error import err_wrap
import pyspark


@err_wrap
def predict(sc, in1, in2, **params):
    if isinstance(in1, pyspark.RDD):
        temp = in2.predict(in1)
    else:
        temp = in1.predict(in2)
    return True, temp
