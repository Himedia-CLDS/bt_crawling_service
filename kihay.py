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
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import re

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# WebDriver Manager의 로깅 레벨을 설정
wdm_logger = logging.getLogger("WDM")
wdm_logger.setLevel(logging.WARNING)  # WARNING 이상의 로그만 출력

# security.json 파일열기
with open('security.json', 'r') as security:
    config = json.load(security)

# 브라우저 열기
options = Options()
options.headless = True  # Rambda에서는 GUI를 지원하지 않음
options.add_experimental_option(
    'excludeSwitches', ['enable-logging'])  # webdriver 로그뺴기
service = Service(EdgeChromiumDriverManager().install())

headers = {'Content-Type': 'application/json'}

def crawling_main():

    logging.info("START crawling_main()")

    try:
        driver = webdriver.Edge(service=service, options=options)
        error_id =[]
        # 크롤링할 URL
        kihay = config["kihay"]
        driver.get(kihay["url"])

        # 페이지 로드 대기 (필요 시 explicit wait을 사용)
        driver.implicitly_wait(10)

        # 가져올 데이터 상위태그
        items = driver.find_elements(By.CSS_SELECTOR, 'div.img_box')

        urls = []
        products = []

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
                logging.info(f"[{i}] Error processing item: {e}")

        # 상세URL 데이터 꺼내기
        driver = webdriver.Edge(service=service, options=options)
        in_items = driver.find_elements(By.CSS_SELECTOR, 'div.img_box')
        products_id = None
        for i, url in enumerate(urls):
            try:
                driver.get(url)
                
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
                h2_element = driver.find_element((By.ID, 'md-s-comment'))
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
                if i == 4 and i == 7 and i == 12:
                    products.append({
                        "_index": "bul$#$%$#",
                        "_id": products_id,
                        "_source": product
                    })
                else:
                    products.append({
                        "_index": "bulk_api_test",
                        "_id": products_id,
                        "_source": product
                    })

                # n건당 크롤링 데이터적재알림
                if i != 0 and i % 10 == 0:
                    noti = {
                        "channel": kihay["channel"],
                        "text": f"크롤링 진행중 {i}건"
                    }
                    slack(noti)

            except Exception as e:
                logging.info(f"[{i}] {products_id}")
                logging.info(f"[{i}] Error processing item: {e}")
                error_id.append({
                    "_index": "none_element",
                    "_id": products_id
                })

    except Exception as e:
        logging.info(f"Failed to set up WebDriver: {e}")
    finally:
        driver.quit()

    print(f"element: {len(error_id)}")

    # Elasticsearch 설정
    es_config = config['es']
    es = Elasticsearch(
        [es_config['es_url']],
        http_auth=(es_config['username'], es_config['password'])
    )

    ##### Bulk API를 사용하여 데이터 전송 #####
    fail_count = 0
    noti = {}

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
            "channel": kihay["channel"],
            "text": f"성공 {success}건\n실패 {fail_count}건\n누락{len(error_id)}"
        }

    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
        fail_count = len(e.errors)
        noti = {
            "channel": kihay["channel"],
            "text": f"Error 크롤링 중단({fail_count})\n{e}"
        }
        for error in e.errors:
            logging.error(error)

    print(noti.get("text"))
    slack(noti)


# 슬랙 알림보내기
def slack(noti):
    slack_config = config['slack']

    response = requests.post(slack_config['url'], headers=headers, json=noti)
    if response.status_code != 200:
        logging.info(f"Failed to send Slack notification. Status code: {response.status_code}, Response: {response.text}")


# "더보기" 버튼 클릭하여 모든 데이터를 로드
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

if __name__ == "__main__":
    crawling_main()

