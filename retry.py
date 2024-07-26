import logging
import json
import requests

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from elasticsearch.exceptions import NotFoundError

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# secyrity.json 파일읽기
with open('security.json', 'r') as security:
    config = json.load(security)

headers = {'Content-Type': 'application/json'}


def crawling_retry():
    try:
        logging.info("START crawling_retry()")
        kihay_config = config['kihay']
        noti = {}

        # 엘라스틱 정보
        es_config = config['es']
        es = Elasticsearch(
            [es_config['es_url']],
            http_auth=(es_config['username'], es_config['password'])
        )

        noti = {
            "channel": kihay_config["channel"],
            "text": "** 크롤링 재시도 **"
        }

        # fail_test_data 인덱스에 데이터가 없을때 까지 반복
        while True:
            logging.info("===== ReTry Start =====")

            # search사용해서 인덱스의 모든데이터 불러오기
            query = {
                "query": {
                    "match_all": {}
                }
            }
            get_data = es.search(index=es_config['fail_index'], body=query)
            datas = get_data["hits"]["hits"]

            if len(datas) == 0:
                noti = {
                    "channel": kihay_config["channel"],
                    "text": "** 크롤링 재시도 알림 **\n데이터가 없습니다."
                }
                break

            # 인덱스 변경
            set_data = []
            for hit in datas:
                data = {
                    "_op_type": "index",
                    "_index": es_config['main_index'],
                    "_id": hit["_id"],
                    "_source": hit["_source"]
                }
                set_data.append(data)

            # 데이터 전송
            get_data = []
            success_id = []
            fail_count = 0
            success, responses = bulk(es, set_data, raise_on_error=False)
            if len(responses) > 0:
                for data in datas:
                    for response in responses:
                        if 'index' in response and response['index']['status'] >= 300:
                            fail_count += 1
                            if data["_index"] == response['index']:
                                temp = {
                                    "_op_type": "index",
                                    "_index": es_config['fail_index'],
                                    "_id": data["_id"],
                                    "_source": data["_source"]
                                }
                                get_data.append(temp)
            else:
                if len(get_data) == 0:
                    for data in set_data:
                        temp = {
                            '_op_type': 'delete',
                            "_index": es_config['fail_index'],
                            "_id": data["_id"]
                        }
                        success_id.append(temp)
                else:
                    for gdata, sdata in zip(get_data, set_data):
                        if gdata["_id"] != sdata["_id"]:
                            temp = {
                                '_op_type': 'delete',
                                "_index": es_config['fail_index'],
                                "_id": sdata["_id"]
                            }
                        success_id.append(temp)

            bulk(es, get_data, raise_on_error=False)
            bulk(es, success_id, raise_on_error=False)
            noti = {
                "channel": kihay_config["channel"],
                "text": "** 크롤링 재시도 알림 **\n크롤링 재시도 성공"
            }

        slack(noti)

    except NotFoundError:
        print(str(NotFoundError))


def slack(noti):
    slack_config = config['slack']
    response = requests.post(slack_config['url'], headers=headers, json=noti)
    if response.status_code != 200:
        logging.info(f"Failed to send Slack notification. Status code: {
                     response.status_code}, Response: {response.text}")


if __name__ == "__main__":
    crawling_retry()
