#!/usr/bin/python
# -*- coding:utf-8 -*-
import json
import time

from ext import DBWorker, DBTask, DBData, rd, cache
from inout import *
from const import SPARK_MASTER

from in0 import *
from in1 import *
from in2 import *

# LogisticRegression xgboost random-forest
# Kmeans dbscan
# apriori fpgrowth prefixspan

import pyspark
import os
import sys


data = dict()


def do_task(task_id, co, db_worker):
    res = db_worker.query(DBTask, DBTask.id == task_id)
    if not res:
        print(task_id, 'not exist in database')
        return False
    if res[0].is_deleted:
        print(task_id, ' is deleted.')
        return True
    task = json.loads(res[0].task)
    cache.user = res[0].user
    sc = pyspark.SparkContext(conf=co)
    result, message = do_with_task(task, sc, db_worker, task_id)
    sc.stop()
    now = time.localtime(time.time())
    print(task_id, message)
    db_worker.update_log(task_id, message)
    if not result:
        db_worker.update_status(task_id, 'Fail', now)
    else:
        db_worker.update_status(task_id, 'Success', now)
    return True


def do_with_task(args, sc, db_worker, task_id):
    res, dic, lines_in, lines_out = task_split(args)
    for each in res['in0']:
        result, tmp = do(each, dic, lines_in, lines_out, sc, task_id, db_worker)
        if not result:
            return False, tmp
    return True, 'Success'


def do(t, dic, lines_in, lines_out, sc, task_id, db_worker):
    db_worker.update_status(task_id, 'running step:' + t)
    global data
    if dic[t]['node_type'] in in0out1:
        result, tmp = run_func(dic[t]['node_type'], sc=sc, params=dic[t]['params'])
        if not result:
            sc.cancelAllJobs()
            return False, tmp
        data[t + 'out1'] = tmp
        if isinstance(tmp, pyspark.RDD):
            temp = tmp.first()
            db_worker.insert(DBData(
                task_id=task_id,
                step=t,
                data=repr(temp),
            ))
        elif isinstance(tmp, int) or isinstance(tmp, str):
            db_worker.insert(DBData(
                task_id=task_id,
                step=t,
                data=str(tmp),
            ))
        for each in lines_out[t + 'out1']:
            result, tmp = do(each[:-3], dic, lines_in, lines_out, sc, task_id, db_worker)
            if not result:
                return result, tmp
    elif dic[t]['node_type'] in in1out0:
        if lines_in[t + 'in1'] not in data:
            return False, 'run ' + t + ' but ' + lines_in[t + 'in1'] + ' not in data'
            # continue
        result, tmp = run_func(dic[t]['node_type'], sc=sc, params=dic[t]['params'], in1=data[lines_in[t + 'in1']])
        if not result:
            sc.cancelAllJobs()
            return False, tmp
    elif dic[t]['node_type'] in in1out1:
        if lines_in[t + 'in1'] not in data:
            return False, 'run ' + t + ' but ' + lines_in[t + 'in1'] + ' not in data'
            # continue
        result, tmp = run_func(dic[t]['node_type'], sc=sc, params=dic[t]['params'], in1=data[lines_in[t + 'in1']])
        if not result:
            sc.cancelAllJobs()
            return False, tmp
        data[t + 'out1'] = tmp
        if isinstance(tmp, pyspark.RDD):
            temp = tmp.first()
            db_worker.insert(DBData(
                task_id=task_id,
                step=t,
                data=repr(temp),
            ))
        elif isinstance(tmp, int) or isinstance(tmp, str):
            db_worker.insert(DBData(
                task_id=task_id,
                step=t,
                data=str(tmp),
            ))
        for each in lines_out[t + 'out1']:
            result, tmp = do(each[:-3], dic, lines_in, lines_out, sc, task_id, db_worker)
            if not result:
                return result, tmp
    elif dic[t]['node_type'] in in1out2:
        if lines_in[t + 'in1'] not in data:
            return False, 'run ' + t + ' but ' + lines_in[t + 'in1'] + ' not in data'
            # continue
        result, tmp = run_func(dic[t]['node_type'], sc=sc, params=dic[t]['params'], in1=data[lines_in[t + 'in1'][0]])
        if not result:
            sc.cancelAllJobs()
            return False, tmp
        data[t + 'out1'] = tmp[0]
        data[t + 'out2'] = tmp[1]
        if isinstance(tmp[0], pyspark.RDD):
            temp = tmp[0].first()
            db_worker.insert(DBData(
                task_id=task_id,
                step=t,
                data=repr(temp),
            ))
        elif isinstance(tmp[0], int) or isinstance(tmp[0], str):
            db_worker.insert(DBData(
                task_id=task_id,
                step=t,
                data=str(tmp[0]),
            ))
        if isinstance(tmp[1], pyspark.RDD):
            temp = tmp[1].first()
            db_worker.insert(DBData(
                task_id=task_id,
                step=t,
                data=repr(temp),
            ))
        elif isinstance(tmp[1], int) or isinstance(tmp[1], str):
            db_worker.insert(DBData(
                task_id=task_id,
                step=t,
                data=str(tmp[1]),
            ))
        for each in lines_out[t + 'out1']:
            result, tmp = do(each[:-3], dic, lines_in, lines_out, sc, task_id, db_worker)
            if not result:
                return result, tmp
        for each in lines_out[t + 'out2']:
            result, tmp = do(each[:-3], dic, lines_in, lines_out, sc, task_id, db_worker)
            if not result:
                return result, tmp
    elif dic[t]['node_type'] in in2out1:
        if lines_in[t + 'in1'] not in data and lines_in[t + 'in2'] not in data:
            return False, 'run ' + t + ' but ' + lines_in[t][0] + ' or ' + lines_in[t][1] + ' not in data'
            # continue
        if lines_in[t + 'in1'] not in data or lines_in[t + 'in2'] not in data:
            return True, None
        result, tmp = run_func(dic[t]['node_type'], sc=sc, params=dic[t]['params'], in1=data[lines_in[t + 'in1']], in2=data[lines_in[t + 'in2']])
        if not result:
            sc.cancelAllJobs()
            return False, tmp
        data[t + 'out1'] = tmp
        if isinstance(tmp, pyspark.RDD):
            temp = tmp.first()
            db_worker.insert(DBData(
                task_id=task_id,
                step=t,
                data=repr(temp),
            ))
        elif isinstance(tmp, int) or isinstance(tmp, str):
            db_worker.insert(DBData(
                task_id=task_id,
                step=t,
                data=str(tmp),
            ))
        for each in lines_out[t + 'out1']:
            result, tmp = do(each[:-3], dic, lines_in, lines_out, sc, task_id, db_worker)
            if not result:
                return result, tmp
    elif dic[t]['node_type'] in in2out2:
        if lines_in[t + 'in1'] not in data and lines_in[t + 'in2'] not in data:
            return False, 'run ' + t + ' but ' + lines_in[t + 'in1'] + ' or ' + lines_in[t + 'in2'] + ' not in data'
            # continue
        if lines_in[t + 'in1'] not in data or lines_in[t + 'in2'] not in data:
            return True, None
        result, tmp = run_func(dic[t]['node_type'], sc=sc, params=dic[t]['params'], in1=data[lines_in[t + 'in1']], in2=data[lines_in[t + 'in2']])
        if not result:
            sc.cancelAllJobs()
            return False, tmp
        data[t + 'out1'] = tmp[0]
        data[t + 'out2'] = tmp[1]
        if isinstance(tmp[0], pyspark.RDD):
            temp = tmp[0].first()
            db_worker.insert(DBData(
                task_id=task_id,
                step=t,
                data=repr(temp),
            ))
        elif isinstance(tmp[0], int) or isinstance(tmp[0], str):
            db_worker.insert(DBData(
                task_id=task_id,
                step=t,
                data=str(tmp[0]),
            ))
        if isinstance(tmp[1], pyspark.RDD):
            temp = tmp[1].first()
            db_worker.insert(DBData(
                task_id=task_id,
                step=t,
                data=repr(temp),
            ))
        elif isinstance(tmp[1], int) or isinstance(tmp[1], str):
            db_worker.insert(DBData(
                task_id=task_id,
                step=t,
                data=str(tmp[1]),
            ))
        for each in lines_out[t + 'out1']:
            result, tmp = do(each[:-3], dic, lines_in, lines_out, sc, task_id, db_worker)
            if not result:
                return result, tmp
        for each in lines_out[t + 'out2']:
            result, tmp = do(each[:-3], dic, lines_in, lines_out, sc, task_id, db_worker)
            if not result:
                return result, tmp
    else:
        return False, 'Invalid inout type!' + t
    return True, None


def task_split(args):
    res = {
        'in0': set(),
        'in1': set(),
        'in2': set(),
    }
    dic = dict()
    lines_in = dict()
    lines_out = dict()
    for line, pair in args['all_lines'].items():
        lines_in[pair[1]] = pair[0]
        if pair[0] not in lines_out:
            lines_out[pair[0]] = [pair[1], ]
        else:
            lines_out[pair[0]].append(pair[1])
    for node, t in args['all_nodes'].items():
        dic[node] = {
                'name': node,
                'node_type': t,
                'params': args['nodes_details'][node],
        }
        if t in in0out1:
            res['in0'].add(node)

        elif t in in1out0 or t in in1out1 or t in in1out2:
            res['in1'].add(node)

        elif t in in2out1 or t in in2out2:
            res['in2'].add(node)

        else:
            print("ERROR, unknown type found : " + t)
            return False
    return res, dic, lines_in, lines_out


def run_func(node_type, params=None, sc=None, in1=None, in2=None):
    s = node_type.replace('-', '_')
    return eval(s + '(sc=sc, in1=in1, in2=in2, **params)')


def run():
    global data
    _sc = pyspark.SparkConf()
    _sc.setMaster(SPARK_MASTER)
    db_worker = DBWorker()
    print('start')
    while True:
        data = dict()
        print('waiting')
        task_id = rd.blpop('task')
        # print(model_id)
        db_worker.update_status(task_id[1].decode(), 'running')
        try:
            do_task(task_id[1].decode(), _sc, db_worker)
        except Exception as E:
            print(E)
            now = time.localtime(time.time())
            db_worker.update_status(task_id[1].decode(), 'Fail', now)


if __name__ == '__main__':
    try:
        run()
    except Exception as e:
        print(e)
    python = sys.executable
    os.execl(python, python, *sys.argv)
