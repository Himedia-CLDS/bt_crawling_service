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
logger = logging.getLogger("test")

def crawling_main():

    logging.info("START crawling_main()")
    # 크롤링할 브라우저
    options = Options()
    options.headless = True  # Rambda에서는 GUI를 지원하지 않음
    # webdriver 로그뺴기
    options.add_experimental_option('excludeSwitches', ['enable-logging'])
    service = Service(EdgeChromiumDriverManager().install())


    # security.json 파일열기
    with open('security.json', 'r') as security:
        config = json.load(security)
    try:
        # 브라우저 열기
        driver = webdriver.Edge(service=service, options=options)

        # 크롤링할 URL
        kihay = config["kihay"]
        driver.get(kihay["url"])

        # 페이지 로드 대기 (필요 시 explicit wait을 사용)
        driver.implicitly_wait(10)

        # "더보기" 버튼 클릭하여 모든 데이터를 로드
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
                # print("No more '더보기' button or error occurred:", e)
                try:
                    alert = Alert(driver)
                    alert.accept()
                    print("Alert accepted")
                except:
                    break

        # 가져올 데이터 상위태그
        items = driver.find_elements(By.CSS_SELECTOR, 'div.img_box')

        urls = []
        products = []

        # 상세정보URL가져오기
        for i, item in enumerate(items):
            if i >= 10:
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


        for i, url in enumerate(urls):
            try:
                print(f">>>>>>>>>>>>>>>>>>>>>>>>>진행중URL: {url}")
                driver.get(url)
                WebDriverWait(driver, 20).until(
                    EC.presence_of_element_located((By.ID, 'product_name')))
                
                ko_name_element = driver.find_element(By.ID, 'product_name')

                en_name_element = driver.find_element(
                    By.ID, 'product_englishname')

                price_div_element = driver.find_element(
                    By.CSS_SELECTOR, 'div.price_box')

                price_element = price_div_element.find_element(
                    By.CSS_SELECTOR, 'div')

                img_element = driver.find_element(
                    By.CSS_SELECTOR, 'img.middle')

                categori_element = driver.find_element(
                    By.XPATH, ".//li[span[contains(text(),'종류')]]")

                alcohol_element = driver.find_element(
                    By.XPATH, ".//li[span[contains(text(),'도수')]]")

                country_element = driver.find_element(
                    By.XPATH, ".//li[span[contains(text(),'국가')]]")

                capacity_element = driver.find_element(
                    By.XPATH, ".//li[span[contains(text(),'용량')]]")

                h2_element = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.ID, 'md-s-comment')))

                p_element = h2_element.find_element(
                    By.XPATH, 'following-sibling::p')

                aroma_element = driver.find_element(
                    By.XPATH, ".//li[span[contains(text(),'Aroma')]]")

                taste_element = driver.find_element(
                    By.XPATH, ".//li[span[contains(text(),'Taste')]]")

                finish_element = driver.find_element(
                    By.XPATH, ".//li[span[contains(text(),'Finish')]]")


                ko_name = ko_name_element.text.strip() if ko_name_element else 'No name'
                en_name = en_name_element.text.strip() if en_name_element else 'No name'
                price = price_element.text.strip() if price_element else 'No price'
                img_src = img_element.get_attribute(
                    'src') if img_element else 'No image'
                category = categori_element.text.replace(
                    '종류', '') if categori_element else 'No dosage'
                alcohol = alcohol_element.text.replace(
                    '도수', '') if alcohol_element else 'No dosage'
                country = country_element.text.replace(
                    '국가', '') if country_element else 'No dosage'
                capacity = capacity_element.text.replace(
                    '용량', '') if capacity_element else 'No dosage'
                description = h2_element.text.strip() + " " + \
                    p_element.text.strip() if h2_element and p_element else 'No description'
                aroma = aroma_element.text.replace(
                    'Aroma', '') if aroma_element else 'No dosag'
                taste = taste_element.text.replace(
                    'Taste', '') if taste_element else 'No dosag'
                finish = finish_element.text.replace(
                    'Finish', '') if finish_element else 'No dosag'


                # url로 id 얻어오기
                parsed_url = urllib.parse.urlparse(url)
                params = urllib.parse.parse_qs(parsed_url.query)
                products_id = params.get('goodsNo', ['No goodsNo'])[0]

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
                if(i == 2 or i == 8) : 
                    products.append({
                        "_index": "#$$%&#^@*$",
                        "_id": products_id,
                        "_source": product
                    })
                else:
                    products.append({
                        "_index": "bulk_api_test",
                        "_id": products_id,
                        "_source": product
                    })

            except Exception as e:
                logging.info(f"[{i}] Error processing item: {e}")

    except Exception as e:
        logging.info(f"Failed to set up WebDriver: {e}")
    finally:
        driver.quit()



    # Elasticsearch 설정
    es_config = config['es']
    headers = {'Content-Type': 'application/json'}

    es = Elasticsearch(
        [es_config['es_url']],
        http_auth=(es_config['username'], es_config['password'])
    )

    # 실패정보 받을 수 있는지 ###############
    # 실패데이터를 뽑을 수 있는지 ###########
    # 넣은데이터보다 적제되는데이터가 적음 insert, delete 확인해보기
    # 넣는데이터 로그남겨서 ES랑 비교하기

    ##### Bulk API를 사용하여 데이터 전송 #####
    fail_count = 0
    re_fail_count = 0

    errors = []
    retry_products = []
    retry = 0

    # 슬렉
    slack_config = config['slack']
    noti = {}

    try:
        # 첫데이터 집어넣기
        success, responses = bulk(es, products, raise_on_error=False)
        for response in responses:
            if 'index' in response and response['index']['status'] >= 300:
                fail_count += 1
                errors.append(response)

        # 실패한 데이터를 fail_test_data 인덱스에 저장
        if fail_count > 0:
            fail_data = []
            for error in errors:
                for product in products:
                    if product['_id'] == error['index']['_id']:
                        fail_data.append({
                            "_index": "fail_test_data",
                            "_id": product['_id'],
                            "_source": product['_source']
                        })

            # 실패 데이터를 fail_test_data 인덱스에 저장
            bulk(es, fail_data, raise_on_error=False)

        # 재도전
        while retry < 3:
            # 실패데이터 없으면 종료
            if fail_count == 0:
                break

            retry += 1
            retry_products = []
            for error in errors:
                for product in products:
                    if product['_id'] == error['index']['_id']:
                        retry_products.append({
                            "_index": "bulk_api_test",
                            "_id": product['_id'],
                            "_source": product['_source']
                        })

            # 실패 데이터를 다시 기존 인덱스에 저장
            re_success, re_responses = bulk(es, retry_products, raise_on_error=False)
            re_fail_count = 0  # 재실패 카운트 초기화
            errors = []  # errors 초기화

            for response in re_responses:
                if 'index' in response and response['index']['status'] >= 300:
                    re_fail_count += 1
                    errors.append(response)

            if re_fail_count == 0:
                break

        noti = {
            "channel": kihay["channel"],
            "text": f"성공 {success}건\n실패 {fail_count}건\n재시도 {retry}\n재시도 성공 {re_success}\n재시도 실패 {re_fail_count}"
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

    # 슬렉 알림보내기
    # response = requests.post(slack_config['url'], headers=headers, json=noti)
    # if response.status_code != 200:
    #     logging.info(f"Failed to send Slack notification. Status code: {response.status_code}, Response: {response.text}")


if __name__ == "__main__":
    crawling_main()

