import logging
import json

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("test")

def crawling_retry():

    logging.info("START crawling_retry()")

    # secyrity.json 파일읽기
    with open('security.json', 'r') as security:
        config = json.load(security)
    
    # 엘라스틱 정보
    es_config = config['es']
    es = Elasticsearch(
        [es_config['es_url']],
        http_auth=(es_config['username'], es_config['password'])
    )

    get_data = es.get(index = "fail_test_data")
    print(get_data)
    



if __name__ == "__main__":
    crawling_retry()