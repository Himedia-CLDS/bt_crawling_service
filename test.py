import logging
import json
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk, BulkIndexError

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("test")


def lambda_handler():
    logging.info("START lambda_handler()")

    try:
        products = []

        for i in range(10):
            product = {
                "kor_name": f"kor_name[{i}]"
            }

            # 잘못된 인덱스 이름 사용으로 실패데이터 생성
            if i == 3 or i == 5 or i == 8:
                products.append({
                    "_index": "invalid_index_name~!@#",
                    "_id": f"test_id_{i}",
                    "_source": product
                })
            else:
                products.append({
                    "_index": "fail_test",
                    "_id": f"test_id_{i}",
                    "_source": product
                })

    except Exception as e:
        logging.info(f"Failed to set up WebDriver: {e}")

    with open('security.json', 'r') as security:
        config = json.load(security)

    # Elasticsearch 설정
    es_config = config['es']
    headers = {'Content-Type': 'application/json'}

    es = Elasticsearch(
        [es_config['es_url']],
        http_auth=(es_config['username'], es_config['password'])
    )

    print("===========================")
    for i in products:
        print(i.get("_source"))


    ##### Bulk API를 사용하여 데이터 전송 #####
    success_count = 0
    re_success_count = 0
    fail_count = 0
    re_fail_count = 0
    re_try_count = 0

    errors = []
    retry_products = []
    retry = 3
    
    try:
        # 첫데이터 집어넣기
        success, responses = bulk(es, products, raise_on_error=False)
        for response in responses:
            if 'index' in response and response['index']['status'] >= 300:
                fail_count += 1
                errors.append(response)
            else:
                success += 1
            
        # 확인용 코드
        print(f"성공 {success}건 / 실패 {fail_count}건")
        success_count = success

        # 재도전
        while retry > 0:
            # 실패데이터가 없으면 종료
            if fail_count == 0:
                retry = 0
            else:
                retry -= 1
                re_try_count += 1
                for error in errors:
                    logging.info(error['index']['error'])

                    # 실패한 id비교후 실패데이터만 retry에 저장 index 수정
                    for product in products:
                        if product['_id'] == error['index']['_id']:
                            product['_index'] = 'fail_test_data'
                            retry_products.append(product)
                        
                
                # 실패데이터 실패인덱스에 저장
                success, responses = bulk(es, retry_products, raise_on_error=False)
                for response in responses:
                    if 'index' in response and response['index']['status'] >= 300:
                        re_fail_count += 1
                        errors.append(response)
                    else:
                        re_success_count += 1
                # 확인용 코드
                if re_fail_count == 0:
                    break
        re_success_count = success
        print(f"재성공 {re_success_count}건 / 실패 {re_fail_count}건")
                
            

    except BulkIndexError as e:
        logging.error(f"Bulk indexing error: {e}")
        fail_count = len(e.errors)
        for error in e.errors:
            logging.error(error)
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")

        
    # 확인용 코드
    print(f"===================")
    print(f"성공 {success_count}건\n실패 {fail_count}건")
    print(f"재도전 {re_try_count}건\n재도전 성공 {re_success_count}")
    print(f"===================")

if __name__ == "__main__":
    lambda_handler()
