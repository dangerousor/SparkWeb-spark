#!/usr/bin/python
# -*- coding:utf-8 -*-
from error import err_wrap
from const import HDFS_PATH
from pyspark.mllib.clustering import KMeansModel
from pyspark.mllib.fpm import FPGrowthModel
from pyspark.mllib.classification import LogisticRegressionModel


@err_wrap
def data_instream(sc, **params):
    if params['type'] == 'file':
        text_file = sc.textFile(HDFS_PATH + params['user'] + '/data/' + params['path'])
    elif params['type'] == 'folder':
        text_file = sc.wholeTextFiles(HDFS_PATH + params['user'] + '/data/' + params['path'])
    else:
        raise Exception("Invalid data type!")
    text_file = text_file.map(lambda row: row.encode(params['charset']).replace('\n', '').split(params['separator']))
    return True, text_file


@err_wrap
def model_instream(sc, **params):
    if params['type'] == 'kmeans':
        model = KMeansModel.load(HDFS_PATH + params['user'] + '/model/' + params['path'])
    elif params['type'] == 'fpgrowth':
        model = FPGrowthModel.load(HDFS_PATH + params['user'] + '/model/' + params['path'])
    elif params['type'] == 'logistic-regression':
        model = LogisticRegressionModel.load(HDFS_PATH + params['user'] + '/model/' + params['path'])
    else:
        raise Exception("Invalid model path!")
    return True, model
