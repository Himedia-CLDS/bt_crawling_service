import requests
import logging
import json
import urllib.parse
from selenium import webdriver
from selenium.webdriver.common.alert import Alert
from selenium.webdriver.edge.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.edge.service import Service
from webdriver_manager.microsoft import EdgeChromiumDriverManager
from selenium.webdriver.support import expected_conditions as EC
from elasticsearch.exceptions import NotFoundError
import time
import re
import schedule

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

##### 로그설정
log_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='[%Y-%m-%d %H:%M:%S]')
c_log = logging.getLogger(name='CrawlingLog')
c_log.setLevel(logging.INFO)
logfile_handler = logging.FileHandler("./logs/crawling_kihay.log", encoding='utf-8')
logfile_handler.setFormatter(log_formatter)
c_log.addHandler(logfile_handler)

# WebDriver Manager의 로깅 레벨을 설정
wdm_logger = logging.getLogger("WDM")
wdm_logger.setLevel(logging.WARNING)  # WARNING 이상의 로그만 출력

# security.json 파일열기
with open('security.json', 'r') as security:
    config = json.load(security)

##### 브라우저설정
options = Options()
options.headless = True  # Rambda에서는 GUI를 지원하지 않음
options.add_experimental_option(
    'excludeSwitches', ['enable-logging'])  # webdriver 로그뺴기
service = Service(EdgeChromiumDriverManager().install())

headers = {'Content-Type': 'application/json'}
slack_config = config['slack']



########## 크롤링 ##########
def crawling_main():
    c_log.info(">>> 크롤링 시작")

    noti = {
        "channel": slack_config["channel"],
        "text": "** 크롤링 시작 **"
    }
    slack(noti)
    
    try:    
        
        # 크롤링할 URL
        kihay = config["kihay"]
        
        # 페이지 로드 대기 (필요 시 explicit wait을 사용)
        driver = webdriver.Edge(service=service, options=options)
        driver.implicitly_wait(10)
        driver.get(kihay["url"])
        c_log.info(">>>>>>> driver")

        # 가져올 데이터 상위태그
        items = driver.find_elements(By.CSS_SELECTOR, 'div.img_box')

        urls = []
        products = []
        error_id = []

        c_log.info(">>> 상세 URL 수집중...")
        # 상세정보URL가져오기
        for i, item in enumerate(items):
            if len(urls) == 15:
                break
            try:
                url_element = item.find_element(By.CSS_SELECTOR, 'a')
                product_url = url_element.get_attribute(
                    'href') if url_element else 'No URL'

                urls.append(product_url)
                
            except Exception as e:
                c_log.info(f"[{i}] Error processing item: {e}")


        # 상세URL 데이터 꺼내기
        driver = webdriver.Edge(service=service, options=options)
        in_items = driver.find_elements(By.CSS_SELECTOR, 'div.img_box')
        products_id = None
        for i, url in enumerate(urls):
            try:
                driver.get(url)
                driver.implicitly_wait(10)
                
                # 요소찾기
                ko_name_element = driver.find_element(By.ID, 'product_name')
                en_name_element = driver.find_element(By.ID, 'product_englishname')
                price_div_element = driver.find_element(By.CSS_SELECTOR, 'div.price_box')
                price_element = price_div_element.find_element(By.CSS_SELECTOR, 'div')
                img_element = driver.find_element(By.CSS_SELECTOR, 'img.middle')
                categori_element = driver.find_element(By.XPATH, ".//li[span[contains(text(),'종류')]]")
                alcohol_element = driver.find_element(By.XPATH, ".//li[span[contains(text(),'도수')]]")
                country_element = driver.find_element(By.XPATH, ".//li[span[contains(text(),'국가')]]")
                capacity_element = driver.find_element(By.XPATH, ".//li[span[contains(text(),'용량')]]")
                h2_element = driver.find_element(By.ID, 'md-s-comment')
                p_element = h2_element.find_element(By.XPATH, 'following-sibling::p')
                aroma_element = driver.find_element(By.XPATH, ".//li[span[contains(text(),'Aroma')]]")
                taste_element = driver.find_element(By.XPATH, ".//li[span[contains(text(),'Taste')]]")
                finish_element = driver.find_element(By.XPATH, ".//li[span[contains(text(),'Finish')]]")

                # 필요한 정보 꺼내기
                ko_name = ko_name_element.text.strip() if ko_name_element else 'No name'
                en_name = en_name_element.text.strip() if en_name_element else 'No name'
                price = price_element.text.strip() if price_element else 'No price'
                img_src = img_element.get_attribute('src') if img_element else 'No image'
                category = categori_element.text.replace('종류', '') if categori_element else 'No dosage'
                
                if "위스키" in category.lower():
                    alcohol = alcohol_element.text.replace('도수', '') if alcohol_element else 'No dosage'
                    country = country_element.text.replace('국가', '') if country_element else 'No dosage'
                    capacity = capacity_element.text.replace('용량', '') if capacity_element else 'No dosage'
                    description = h2_element.text.replace("'MD's Comment", '') + " " + \
                        p_element.text.strip() if h2_element and p_element else 'No description'
                    aroma = aroma_element.text.replace('Aroma', '') if aroma_element else 'No dosag'
                    taste = taste_element.text.replace('Taste', '') if taste_element else 'No dosag'
                    finish = finish_element.text.replace('Finish', '') if finish_element else 'No dosag'

                # url로 id 얻어오기
                parsed_url = urllib.parse.urlparse(url)
                params = urllib.parse.parse_qs(parsed_url.query)
                products_id = params.get('goodsNo', ['No goodsNo'])[0]

                # 가격 int형 변환
                price = int(re.sub(r'[^0-9]', '', price))

                product = {
                    "product_id": products_id,
                    "kor_name": ko_name,
                    "eng_name": en_name,
                    "price": price,
                    "img": img_src,
                    "alcohol": alcohol,
                    "country": country,
                    "capacity": capacity,
                    "description": description,
                    "category": category,
                    "tasting_notes": {
                        "aroma": aroma,
                        "taste": taste,
                        "finish": finish
                    }
                }
                products.append({
                    "_index": "bulk_api_test",
                    "_id": products_id,
                    "_source": product
                })
                c_log.info(f"[{i}]products")

                # n건당 크롤링 데이터적재알림
                if i != 0 and i % 10 == 0:
                    c_log.info(f">>> 크롤링 진행중 {i}건")
                    noti = {
                        "channel": slack_config["channel"],
                        "text": f"크롤링 진행중 {i}건"
                    }
                    slack(noti)

            except Exception as e:
                c_log.info(f"[{i}] {products_id}")
                c_log.info(f"[{i}] Error processing item: {e}")
                error_id.append({
                    "_index": "none_element",
                    "_id": products_id
                })
        driver.quit()
    except Exception as e:
        c_log.info(f"Failed to set up WebDriver: {e}")
        

    # Elasticsearch 설정
    es_config = config['es']
    es = Elasticsearch(
        [es_config['es_url']],
        http_auth=(es_config['username'], es_config['password'])
    )

    ##### Bulk API를 사용하여 데이터 전송 #####
    fail_count = 0

    try:
        # 첫데이터 집어넣기
        success, responses = bulk(es, products, raise_on_error=False)
        for response in responses:
            if 'index' in response and response['index']['status'] >= 300:
                fail_count += 1
                
        # 실패한 데이터를 fail_test_data 인덱스에 저장
        if fail_count > 0:
            fail_data = []
            for response in responses:
                for product in products:
                    if product['_id'] == response['index']['_id']:
                        fail_data.append({
                            "_index": "fail_test_data",
                            "_id": product['_id'],
                            "_source": product['_source']
                        })

            # 실패 데이터를 fail_test_data 인덱스에 저장
            bulk(es, fail_data, raise_on_error=False)
            # 요소를 찾지 못해서 적재실패한데이터 인덱스에 따로저장
            bulk(es, error_id, raise_on_error=False)


        noti = {
            "channel": slack_config["channel"],
            "text": f"성공 {success}건\n실패 {fail_count}건\n누락{len(error_id)}"
        }

    except Exception as e:
        c_log.error(f"An unexpected error occurred: {e}")
        noti = {
            "channel": slack_config["channel"],
            "text": f"Error 크롤링 중단\n{e}"
        }

    c_log.info(">>> 크롤링 완료 ")
    slack(noti)


########## 실패데이터 재적재 ##########
def crawling_retry():
    c_log.info("===== START crawling_retry =====")

    noti = {
        "channel": slack_config["channel"],
        "text": "*** 실패데이터 재적재 시작 ***"
    }
    slack(noti)

    try:

        # 엘라스틱 정보
        es_config = config['es']
        es = Elasticsearch(
            [es_config['es_url']],
            http_auth=(es_config['username'], es_config['password'])
        )

        noti = {
            "channel": slack_config["channel"],
            "text": "*** 크롤링 재시도 ***"
        }

        # fail_test_data 인덱스에 데이터가 없을때 까지 반복
        while True:

            # search사용해서 인덱스의 모든데이터 불러오기
            query = {
                "query": {
                    "match_all": {}
                }
            }
            get_data = es.search(index=es_config['fail_index'], body=query)
            datas = get_data["hits"]["hits"]

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
                            temp = {
                                '_op_type': 'delete',
                                "_index": es_config['fail_index'],
                                "_id": response['index']['_id']
                            }
                            success_id.append(temp)
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

            if len(datas) == 0:
                noti = {
                    "channel": slack_config["channel"],
                    "text": "*** 크롤링 재시도 알림 ***\n데이터가 없습니다."
                }
                break

            noti = {
                "channel": slack_config["channel"],
                "text": "*** 크롤링 재시도 알림 ***\n크롤링 재시도 성공"
            }

        slack(noti)

    except NotFoundError:
        c_log.info(str(NotFoundError))

########## 슬랙 알림보내기 ##########
def slack(noti):
    response = requests.post(slack_config['url'], headers=headers, json=noti)
    if response.status_code != 200:
        c_log.info(f"Failed to send Slack notification. Status code: {
                   response.status_code}, Response: {response.text}")


########## 더보기버튼처리 ##########
def moreBtn():
    driver = webdriver.Edge(service=service, options=options)
    while True:
        try:
            more_button = driver.find_element(
                By.CSS_SELECTOR, 'button.more_btn')
            if more_button:
                driver.execute_script("arguments[0].click();", more_button)
                time.sleep(2)
            else:
                break
        except Exception as e:
            try:
                alert = Alert(driver)
                alert.accept()
            except:
                break


def main():
    c_log.info("===== START main() =====")
    crawling_main()
    # 매일 at()시에 do(job)함수 실행
    # schedule.every().day.at("01:00").do(crawling_main)
    # schedule.every().day.at("03:00").do(crawling_retry)

    # while True:
    #     # 스케줄러에 등록된작업실행
    #     schedule.run_pending()
    #     time.sleep(1)

if __name__ == "__main__":
    main()

