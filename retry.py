import logging
import json
import requests

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from elasticsearch.exceptions import NotFoundError

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("test")

def crawling_retry():
    try: 
        logging.info("START crawling_retry()")

        headers = {'Content-Type': 'application/json'}

        # secyrity.json 파일읽기
        with open('security.json', 'r') as security:
            config = json.load(security)

        # 슬렉
        slack_config = config['slack']
        noti = {}

        # 엘라스틱 정보
        es_config = config['es']
        es = Elasticsearch(
            [es_config['es_url']],
            http_auth=(es_config['username'], es_config['password'])
        )

        # search사용해서 인덱스의 모든데이터 불러오기
        query = {
            "query": {
                "match_all" : {}
            }
        }
        get_data = es.search(index=es_config['fail_index'], body=query)
        datas = get_data["hits"]["hits"]
        
        # fail_test_data 인덱스에 데이터가 없을때 까지 반복
        while len(get_data) != 0:
            logger.info("===== ReTry Start =====")
            noti = {
                "channel": kihay["channel"],
                "text": f"Error 크롤링 중단({fail_count})\n{e}"
            }

            # 인덱스 변경
            set_data = []
            for hit in datas:
                data = {
                    "_op_type": "index",
                    "_index": es_config['main_index'],
                    "_id" : hit["_id"],
                    "_source": hit["_source"]
                }
                set_data.append(data)

            # 데이터 전송
            get_data = []
            fail_count = 0
            success, responses = bulk(es, set_data, raise_on_error=False)
            for response in responses:
                print(response)
                for data in datas:
                    if 'index' in response and response['index']['status'] >= 300:
                        fail_count += 1
                        if data["_index"] == response['index']:
                            data = {
                                "_op_type": "index",
                                "_index": es_config['fail_index'],
                                "_id": hit["_id"],
                                "_source": hit["_source"]
                            }
                            get_data.append(data)
                    else:
                        print(response['index'])
            bulk(es, get_data, raise_on_error=False)

        # 실패데이터 메인인덱스에 적재완료되면 실패인덱스 삭제    
        es.delete(index=es_config['fail_index'])

        # 슬렉 알림보내기
        response = requests.post(slack_config['url'], headers=headers, json=noti)
        if response.status_code != 200:
            logging.info(f"Failed to send Slack notification. Status code: {response.status_code}, Response: {response.text}")


    except NotFoundError:
        print(str(NotFoundError))



if __name__ == "__main__":
    crawling_retry()