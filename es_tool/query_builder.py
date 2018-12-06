
import os
import sys

_project_root = os.path.dirname(
    os.path.dirname(
        os.path.realpath(__file__)
    )
)
sys.path.append(_project_root)

import json
import requests
from config import configer
index = configer.get('elasticsearch', 'index')
host = configer.get('elasticsearch', 'host')
port = configer.get('elasticsearch', 'port')


class Query:

    def __init__(self):
        self.query = {
            "query": {
                "bool": {
                    "must": [],
                    "should": []
                }
            },
            "aggs": {},
            "_source": [],
            "highlight": {
                "fields": {},
                "pre_tags": ['<span style="color:red">'],
                "post_tags": ['</span>'],
            },
            "size": 10
        }

    def rescore(self, phrase):
        self.query['rescore'] = {"window_size": 5,
                                 "query": {
                                    "rescore_query": {
                                        "match": {
                                            "content": {
                                                "query": phrase,
                                            }
                                        }
                                    }
                                }
                            }

    def tokenize_constant(self, phrase):
        url = 'http://%s:%s/%s/_analyze' % (host, port, index)
        body = {"analyzer": "ik_smart", "text": phrase}
        res = requests.post(url, data=json.dumps(body))
        c = 0
        for r in res.json()['tokens']:
            c += 1
            print("%s %s" % (c, r['token']))
            self.append(constant_score('content', r['token']), 'should')

        # self.rescore(phrase)

    def shingles(self, phrase):
        self.append(match('content', phrase))
        self.append(match('content.shingles', phrase), 'should')

    def sort(self, item, direction='desc'):
        self.query['sort'] = [
            {
                item: {
                    "order": direction
                }
            }
        ]

    def append(self, item, logic='must'):
        self.query["query"]["bool"][logic].append(item)

    def append_aggs(self, item):
        self.query['aggs'].update(item)

    def append_source(self, item):
        self.query['_source'].append(item)

    def highlight(self, item):
        item = {item: {"fragment_size": 100, "number_of_fragments": 5}}
        self.query['highlight']['fields'].update(item)

    def size(self, size):
        self.query['size'] = size

    def dump(self):
        return self.query


def constant_score(key, value):
    return {"constant_score": {
                      "query": {
                          "match": {
                              key: value
                          }
                      }
                }
            }


def fuzzy(key, value):
    return {
            "fuzzy": {
                key: {
                    "value": value,
                    "boost": 1,
                    "fuzziness": 2,
                    "prefix_length": 0,
                    "max_expansions": 100
                }
            }
        }


def agg(key):
    return {
        key: {
            "terms": {
                "field": key,
                "size": 100
            }
        }
    }


def term(key, value):
    return {
        "term": {
            key: value
        }
    }


def terms(key, values):
    return {
        "terms": {
            key: values
        }
    }


def match(key, value, boost=1):
    return {
        "match": {
            key: {
                "query": value,
                "boost": boost,
                # "operator": "and"
            }
        }
    }


def match_phrase(key, value, boost=1, slop=1):
    return {
        "match_phrase": {
            key: {
                "query": value,
                "boost": boost,
                "slop": slop
            }
        }
    }


def default_query():
    query = Query()

    # query.append_source('content')
    query.append_source('title')

    query.highlight('content')

    return query


if __name__ == '__main__':
    q = Query()
    q.tokenize_constant('常熟市海城花苑2幢B219')
