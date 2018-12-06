#!/usr/bin/env python3
# coding=utf-8
import os
import sys
import json
import time, timeit, datetime
from elasticsearch import helpers

_project_root = os.path.dirname(
    os.path.dirname(
        os.path.realpath(__file__)
    )
)
sys.path.append(_project_root)

from model.es import es, es_index, es_type


def clock(func):
    def clocked(*args):
        t0 = timeit.default_timer()
        result = func(*args)
        elapsed = timeit.default_timer() - t0
        name = func.__name__
        arg_str = ', '.join(repr(arg) for arg in args)
        print('[%0.8fs] %s(%s) -> %r documents' % (elapsed, name, arg_str, result))
        return result
    return clocked


def index_it():
    actions = []
    path = '/Users/memect-bridgezhou/Desktop/test_txt'
    for parent, dirnames, filenames in os.walk(path):  # 三个参数：分别返回1.父目录 2.所有文件夹名字（不含路径） 3.所有文件名字
        for filename in filenames:
            p = os.path.join(parent, filename)
            with open(p, 'r') as f:
                action = {
                    "_index": es_index,
                    "_type": es_type,
                    "_source": {
                        'title': filename,
                        'content': f.read()
                    }
                }
                actions.append(action)
    res = helpers.bulk(es.es, actions)
    print(res)


if __name__ == '__main__':
    es.init_es()
    index_it()
