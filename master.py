#!/usr/bin/python
# -*- coding:utf-8 -*-
import json
import time

from basic.ext import DBWorker, DBTask, DBData, rd
from basic.inout import *
from basic.const import SPARK_MASTER

from basic.in0 import *
from basic.in1 import *
from basic.in2 import *

# LogisticRegression xgboost random-forest
# Kmeans dbscan
# apriori fpgrowth prefixspan

import pyspark
import os
import sys


def do_task(task_id, co):
    db_worker = DBWorker()
    res = db_worker.query(DBTask.task, DBTask.id == task_id)
    if not res:
        print(task_id + 'not exist in database')
        return
    print(res)
    task = json.loads(res[0][0])
    sc = pyspark.SparkContext(conf=co)
    result, message = do_with_task(task, sc, db_worker, task_id)
    sc.stop()
    now = time.localtime(time.time())
    print(task_id, message)
    db_worker.update_status(task_id, message, now)
    return 0


def do_with_task(args, sc, db_worker, task_id):
    res, dic, lines = task_split(args)
    data = dict()
    for each in res['in0']:
        result, tmp = run_func(dic[each]['node_type'], sc=sc, params=dic[each]['params'])
        if not result:
            sc.cancelAllJobs()
            return False, tmp
        data[each] = tmp
        if isinstance(tmp, pyspark.RDD):
            temp = tmp.first()
            db_worker.insert(DBData(
                task_id=task_id,
                step=each,
                data=repr(temp),
            ))
        elif isinstance(tmp, int) or isinstance(tmp, str):
            db_worker.insert(DBData(
                task_id=task_id,
                step=each,
                data=str(tmp),
            ))
    for each in res['in1']:
        if len(lines[each]) != 1:
            return False, each + 'linked by ' + str(len(lines[each])) + 'line, need 1'
        if lines[each][0] not in data:
            return False, 'run ' + each + ' but ' + lines[each][0] + ' not in data'
        result, tmp = run_func(dic[each]['node_type'], sc=sc, params=dic[each]['params'], in1=data[lines[each][0]])
        if not result:
            sc.cancelAllJobs()
            return False, tmp
        data[each] = tmp
        if isinstance(tmp, pyspark.RDD):
            temp = tmp.first()
            db_worker.insert(DBData(
                task_id=task_id,
                step=each,
                data=repr(temp),
            ))
        elif isinstance(tmp, int) or isinstance(tmp, str):
            db_worker.insert(DBData(
                task_id=task_id,
                step=each,
                data=str(tmp),
            ))
    for each in res['in2']:
        if len(lines[each]) != 2:
            return False, each + 'linked by ' + str(len(lines[each])) + 'lines, need 2'
        if lines[each][0] not in data or lines[each][1] not in data:
            return False, 'run ' + each + ' but ' + lines[each][0] + ' or ' + lines[each][1] + ' not in data'
        result, tmp = run_func(dic[each]['node_type'], sc=sc, params=dic[each]['params'], in1=data[lines[each][0]], in2=data[lines[each][1]])
        if not result:
            sc.cancelAllJobs()
            return False, tmp
        data[each] = tmp
        if isinstance(tmp, pyspark.RDD):
            temp = tmp.first()
            db_worker.insert(DBData(
                task_id=task_id,
                step=each,
                data=repr(temp),
            ))
        elif isinstance(tmp, int) or isinstance(tmp, str):
            db_worker.insert(DBData(
                task_id=task_id,
                step=each,
                data=str(tmp),
            ))

    return True, 'success'


def task_split(args):
    res = {
        'in0': list(),
        'in1': list(),
        'in2': list(),
    }
    dic = dict()
    lines = dict()
    for line, pair in args['all_lines'].items():
        if pair[1] not in lines:
            lines[pair[1]] = [pair[0], ]
        else:
            lines[pair[1]].append(pair[0])
    for node, t in args['all_nodes'].items():
        dic[node] = {
                'name': node,
                'node_type': t,
                'params': args['nodes_details'][node],
        }
        if t in in0out1:
            res['in0'].append(node)

        elif t in in1out0 or t in in1out1 or t in in1out2:
            res['in1'].append(node)

        elif t in in2out1 or t in in2out2:
            res['in2'].append(node)

        else:
            print("ERROR, unknown type found : " + t)
            return False
    return res, dic, lines


def run_func(node_type, params=None, sc=None, in1=None, in2=None):
    s = node_type.replace('-', '_')
    return eval(s + '(sc=sc, in1=in1, in2=in2, **params)')


if __name__ == '__main__':
    os.environ['PYSPARK_PYTHON'] = sys.executable
    _sc = pyspark.SparkConf()
    _sc.setMaster(SPARK_MASTER)
    print('start')
    while True:
        print('waiting')
        model_id = rd.blpop('task')
        # print(model_id)
        do_task(model_id[1].decode(), _sc)
