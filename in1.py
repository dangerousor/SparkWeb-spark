#!/usr/bin/python
# -*- coding:utf-8 -*-
from .error import err_wrap
from .const import HDFS_PATH
from pyspark.mllib.clustering import KMeans
from pyspark.mllib.fpm import FPGrowth
from pyspark.mllib.classification import LogisticRegressionWithLBFGS
from pyspark.mllib.regression import LabeledPoint


@err_wrap
def data_outstream(sc, in1, **params):
    in1.saveAsTextFile(HDFS_PATH + params['user'] + '/data/' + params['path'])
    return True, None


@err_wrap
def model_outstream(sc, in1, **params):
    in1.save(sc, HDFS_PATH + params['user'] + '/model/' + params['path'])
    return True, None


@err_wrap
def cache(sc, in1, **params):
    in1.cache()
    return True, in1


@err_wrap
def distinct_col(sc, in1, **params):
    temp = in1.map(lambda row: row[int(params['columns'])]).distinct()
    return True, temp


@err_wrap
def distinct_row(sc, in1, **params):
    temp = in1.map(lambda row: set(list(row)))
    return True, temp


@err_wrap
def map(sc, in1, **params):
    temp = in1.map(eval(params['lambda']))
    return True, temp


@err_wrap
def filter(sc, in1, **params):
    temp = in1.filter(eval(params['filter']))
    return True, temp


@err_wrap
def sample(sc, in1, **params):
    temp = in1.sample(False, params['fraction'], 666)
    return True, temp


@err_wrap
def split_col(sc, in1, **params):
    temp = in1.map(lambda x: list(x)[int(params['start']): int(params['end'])])
    return True, temp


@err_wrap
def sort(sc, in1, **params):
    if params['columns'].isdigit():
        temp = in1.sortBy(lambda x: x[int(params['columns'])], ascending=eval(params['ascending']))
        return True, temp
    else:
        raise Exception('Invalid columns!')


@err_wrap
def normalization(sc, in1, **params):
    if params['method'] == 'no':
        temp = in1.map(lambda x: x)
    elif params['method'] == 'int':
        def is_int(v):
            for each in v:
                if not each.isdigit():
                    return False
            return True

        temp = in1.filter(lambda x: is_int(x)).map(lambda x: [int(each) for each in x])
    elif params['method'] == 'float':
        def is_float(v):
            for each in v:
                try:
                    float(each)
                except:
                    return False
            return True

        temp = in1.filter(lambda x: is_float(x)).map(lambda x: [float(each) for each in x])
    else:
        raise Exception('Invalid method!')
    return True, temp


@err_wrap
def kmeans(sc, in1, **params):
    temp = KMeans.train(in1, k=int(params['k']), maxIterations=int(params['maxIterations']))
    return True, temp


@err_wrap
def fpgrowth(sc, in1, **params):
    temp = FPGrowth.train(in1, float(params['minSupport']))
    return True, temp


@err_wrap
def logistic_regression(sc, in1, **params):
    temp = in1.map(lambda x: LabeledPoint(x[int(params['label'])], x[:int(params['label']) + x[int(params['label'])+1:]]))
    temp = LogisticRegressionWithLBFGS.train(temp, iterations=int(params['iterations']), numClasses=int(params['numClasses']))
    return True, temp
