#!/usr/bin/python
# -*- coding:utf-8 -*-
from error import err_wrap
from const import HDFS_PATH
from ext import cache
from pyspark.mllib.clustering import KMeansModel
from pyspark.mllib.fpm import FPGrowthModel
from pyspark.mllib.classification import LogisticRegressionModel
from pyspark.mllib.feature import Word2VecModel
from pyspark.mllib.tree import DecisionTreeModel


@err_wrap
def data_instream(sc, **params):
    if not cache.fs.exists(sc._jvm.org.apache.hadoop.fs.Path(HDFS_PATH + str(cache.user) + '/data/' + params['path'])):
        raise Exception("Invalid file path, path not exists!")
    if params['type'] == 'file':
        text_file = sc.textFile(HDFS_PATH + str(cache.user) + '/data/' + params['path'])
    elif params['type'] == 'folder':
        text_file = sc.wholeTextFiles(HDFS_PATH + str(cache.user) + '/data/' + params['path'])
    else:
        raise Exception("Invalid data type!")
    text_file = text_file.map(lambda row: row.encode(params['charset']).replace('\n', '').split(params['separator']))
    return True, text_file


@err_wrap
def model_instream(sc, **params):
    if not cache.fs.exists(sc._jvm.org.apache.hadoop.fs.Path(HDFS_PATH + str(cache.user) + '/model/' + params['path'])):
        raise Exception("Invalid file path, path not exists!")
    if params['type'] == 'kmeans':
        model = KMeansModel.load(sc, HDFS_PATH + str(cache.user) + '/model/' + params['path'])
    elif params['type'] == 'fpgrowth':
        model = FPGrowthModel.load(sc, HDFS_PATH + str(cache.user) + '/model/' + params['path'])
    elif params['type'] == 'logistic-regression':
        model = LogisticRegressionModel.load(sc, HDFS_PATH + str(cache.user) + '/model/' + params['path'])
    elif params['type'] == 'word2vec':
        model = Word2VecModel.load(sc, HDFS_PATH + str(cache.user) + '/model/' + params['path'])
    elif params['type'] == 'decision-tree':
        model = DecisionTreeModel.load(sc, HDFS_PATH + str(cache.user) + '/model/' + params['path'])
    else:
        raise Exception("Invalid model type!")
    return True, model
