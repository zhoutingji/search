import json
import requests
from model.mysql import con

query = """
    SELECT F015_ZXY050 FROM zxy050 WHERE F004_ZXY050 = '苏州' AND F015_ZXY050 != '' LIMIT %s
"""


def fetch_data_from_mysql():
    with con:
        cur = con.cursor()
        cur.execute(query % (1000, ))
        rows = cur.fetchall()
        return rows


if __name__ == '__main__':
    url = 'http://127.0.0.1:5005/api/search'
    res = fetch_data_from_mysql()
    for r in res:
        data = {"content": "",
                "province": "",
                "city": "",
                "court": "",
                "case_no": r[0]}
        ret = requests.post(url=url, data=json.dumps(data))
        content = ret.json()
        if content['hits']:
            case_no = content['hits'][0]['case_no']
            print(case_no)



