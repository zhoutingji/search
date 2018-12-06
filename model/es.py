import os
import sys
_project_root = os.path.dirname(
    os.path.dirname(
        os.path.realpath(__file__)
    )
)
sys.path.append(_project_root)
import json
from elasticsearch import Elasticsearch
from config import configer


class ES:

    def __init__(self, host=None, port=None, esindex=None, estype=None):
        self.host = host
        self.port = port
        self.esindex = esindex
        self.estype = estype

        url = '{}:{}'.format(self.host, self.port)
        self.es = Elasticsearch([url], timeout=30)

    def init_es(self):
        self.es.indices.create(index=self.esindex, body=index_map)

    def update_it(self, item, id, type):
        return self.es.index(index=self.esindex,
                id=id,
                doc_type=type,
                body=json.dumps(item))

    def update(self, item):
        return self.es.index(index=self.esindex,
                doc_type=self.estype,
                id=item['id'],
                body=json.dumps(item),
                ignore=[413])

    def search(self, body, from_=0, size=10):
        return self.es.search(index=self.esindex,
                doc_type=self.estype,
                body=body,
                from_=from_,
                size=size)

    def delete(self, id_):
        return self.es.delete(index=self.esindex,
                doc_type=self.estype,
                id=id_)

    def get(self, id_):
        return self.es.get(index=self.esindex,
                doc_type=self.estype,
                id=id_)

    def local_update(self, id_, doc):
        body = {
            "doc": doc
        }
        return self.es.update(index=self.esindex,
                doc_type=self.estype,
                id=id_,
                body=body)

    def search_scroll(self, index, doc_type, query):
        try:
            return self.es.search(index=index, doc_type=doc_type, body=query,
                                               search_type="query_then_fetch", scroll="1m")
        except BaseException as e:
            print(str(e))

        return {}

    def scroll_scan(self, query):
        try:
            resJson = self.es.scroll(body=query)
            return resJson
        except BaseException as e:
            print(str(e))

        return []


host = configer.get('elasticsearch', 'host')
port = configer.get('elasticsearch', 'port')

es_index = configer.get('elasticsearch', 'index')
es_type = configer.get('elasticsearch', 'type')

es = ES(host, port, es_index, es_type)


index_map = {
    "settings": {
        "analysis": {
            "filter": {
                "my_shingle_filter": {
                    "type":             "shingle",
                    "min_shingle_size": 2,
                    "max_shingle_size": 2,
                    "output_unigrams":  False,
                    "token_separator": ""
                }
            },
            "analyzer": {
                "my_shingle_analyzer": {
                    "type":             "custom",
                    "tokenizer":        "standard",
                    "filter": ["my_shingle_filter"]
                },
                "my_analyzer": {
                    "tokenizer": "standard"
                }
            }
        }
    },
    "mappings": {
        "document": {
            "properties": {
                "title": {
                    "type": "text",
                    "fields": {
                        "shingles": {
                            "type":     "text",
                            "analyzer": "my_shingle_analyzer"
                        }
                    }
                },
                "content": {
                    "type": "text",
                    "fields": {
                        "shingles": {
                            "type":     "text",
                            "analyzer": "my_shingle_analyzer"
                        }
                    }
                }
            }
        }
    }
}

if __name__ == '__main__':
    es.init_es()