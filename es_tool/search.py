#!/usr/bin/env python3
# coding=utf-8
import re
import json
from model.es import es
from config import configer
from es_tool.query_builder import default_query
from es_tool.query_builder import match, match_phrase, constant_score, fuzzy


def capilize_date(judgement_date):
    try:
        year = judgement_date.split('-')[0]
        month = judgement_date.split('-')[1]
        day = judgement_date.split('-')[2]
        ch_char_dict = {'1': '一', '2': '二', '3': '三', '4': '四', '5': '五',
                        '6': '六', '7': '七', '8': '八', '9': '九', '0': '〇',
                        '01': '一', '02': '二', '03': '三', '04': '四', '05': '五',
                        '06': '六', '07': '七', '08': '八', '09': '九',
                        '10': '十', '11': '十一', '12': '十二', '13': '十三', '14': '十四', '15': '十五', '16': '十六',
                        '17': '十七', '18': '十八', '19': '十九', '20': '二十', '21': '二十一', '22': '二十二', '23': '二十三',
                        '24': '二十四', '25': '二十五', '26': '二十六', '27': '二十七', '28': '二十八', '29': '二十九',
                        '30': '三十', '31': '三十一'}
        year = ''.join([ch_char_dict[i] for i in str(year)])
        month = ch_char_dict[str(month)]
        day = ch_char_dict[str(day)]
        res = '%s年%s月%s日' % (year, month, day)
    except:
        res = ''

    return res


def search_content(data):

    return query_es(data)


def query_es(data=None):
    phrase = data

    offset = 0
    limit = 5

    query_es = default_query()

    if phrase:
        query_es.shingles(phrase)
        # query_es.append(match_phrase('content', phrase, slop=50, boost=10), 'should')
        # query_es.tokenize_constant(phrase)
        # query_es.append(constant_score('content', phrase))
        # query_es.append(match('content', phrase), 'must')
        # query_es.shingles(phrase)
        # query_es.append(fuzzy('content', phrase))
        # query_es.append(match_phrase('content', phrase), 'must')

    query_es_json = query_es.dump()

    print(query_es_json)

    ret = es.search(query_es_json, from_=offset, size=limit)

    ret = response_format(ret)

    print(ret)

    return ret


def response_format(data):

    ret = {
        "search_result": [],
        "total": data['hits']['total']
    }

    # 整理命中条目
    data_hits = data.get("hits", {}).get("hits", [])
    for hit in data_hits:
        hit_new = hit.get('_source')
        hit_new['_score'] = hit.get('_score')
        hit_hl = hit.get("highlight")
        if hit_new:
            hit_new['type'] = 'text'
            hit_new['file_name'] = hit_new['title']
            hit_new['search_result'] = hit_hl['content']
            hit_new['page'] = 0
            ret["search_result"].append(hit_new)

    return ret
