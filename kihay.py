import requests
import logging
import urllib.parse
from selenium import webdriver
from selenium.webdriver.common.alert import Alert
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support import expected_conditions as EC
from elasticsearch.exceptions import NotFoundError
import time
import re
import schedule
import platform
import yaml
import os

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk


class Crawler:

    def __init__(self):
        self.logpath = {}
        self.driver_path = {}
        self.slack_config = {}
        self.es_config = {}
        self.kihay = {}
        self.service = {}

        self.configuration()
        self.setup()

    def setup(self):
        self.log_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='[%Y-%m-%d %H:%M:%S]')
        self.c_log = logging.getLogger(name='CrawlingLog')
        self.c_log.setLevel(logging.INFO)
        self.wdm_logger = logging.getLogger("WDM")
        self.wdm_logger.setLevel(logging.WARNING)

        os_name = platform.system()
        os_name = "ec2" if os_name == "Linux" else "local"
        logfile_handler = logging.FileHandler(
            f"{self.logpath.get(os_name, './')}/crawling.log", encoding='utf-8')
        logfile_handler.setFormatter(self.log_formatter)
        self.service = Service(self.driver_path[os_name])
        
        log_directory = self.logpath.get(os_name, os.path.expanduser('~/crawling-log'))
        if not os.path.exists(log_directory):
            os.makedirs(log_directory)
        
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(self.log_formatter)
        self.c_log.addHandler(console_handler)
        self.c_log.addHandler(logfile_handler)

        self.headers = {'Content-Type': 'application/json'}
    
    def configuration(self):
        with open('config.yml', 'r') as config:
            config = yaml.safe_load(config)

        self.logpath = config['logpath']
        self.driver_path = config['driverpach']
        self.slack_config = config['slack']
        self.es_config = config['es']
        self.kihay = config["kihay"]

        self.es = Elasticsearch(
            [self.es_config['es_url']],
            http_auth=(self.es_config['username'], self.es_config['password'])
        )
        
    def crawling_main(self):
        self.c_log.info(">>> 크롤링 시작")

        self.noti_slack(f"{time.asctime()}\n크롤링 시작")
        try:    
            options = Options()
            options.add_argument('--headless')
            options.add_argument("--single-process")
            options.add_experimental_option('excludeSwitches', ['enable-logging'])
            options.add_argument("--log-level=2")

            driver = webdriver.Chrome(service=self.service, options=options)
            driver.implicitly_wait(10)
            driver.get(self.kihay["url"])

            self.moreBtn(driver)

            items = driver.find_elements(By.CSS_SELECTOR, 'div.img_box')

            urls = []
            products = []
            error_id = []

            self.c_log.info(">>> 상세 URL 수집중...")
            for i, item in enumerate(items):
                try:
                    url_element = item.find_element(By.CSS_SELECTOR, 'a')
                    product_url = url_element.get_attribute(
                        'href') if url_element else 'No URL'

                    urls.append(product_url)

                except Exception as e:
                    self.c_log.info(f"[{i}] Error processing item: {e}")

            driver = webdriver.Chrome(service=self.service, options=options)
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
                    ko_name = ko_name_element.text.strip() if ko_name_element else 'No ko_name'
                    en_name = en_name_element.text.strip() if en_name_element else 'No en_name'
                    price = price_element.text.strip() if price_element else 0
                    img_src = img_element.get_attribute('src') if img_element else 'No image'
                    category = categori_element.text.replace('종류', '') if categori_element else 'No category'

                    if "위스키" in category.lower():
                        alcohol = alcohol_element.text.replace('도수', '') if alcohol_element else 'No alcohol'
                        country = country_element.text.replace('국가', '') if country_element else 'No country'
                        capacity = capacity_element.text.replace('용량', '') if capacity_element else 'No capacity'
                        description = h2_element.text.replace("'MD's Comment", '') + p_element.text.strip() if h2_element and p_element else 'No description'
                        aroma = aroma_element.text.replace('Aroma', '') if aroma_element else 'No aroma'
                        taste = taste_element.text.replace('Taste', '') if taste_element else 'No taste'
                        finish = finish_element.text.replace('Finish', '') if finish_element else 'No finish'

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
                        },
                        "timestamp": time.asctime()
                    }
                    products.append({
                        "_index": self.es_config['main_index'],
                        "_id": products_id,
                        "_source": product
                    })

                    if i != 0 and (i+1) % 30 == 0:
                        self.c_log.info(f">>> 크롤링 진행중 {i+1}건")
                        self.noti_slack(f"크롤링 진행중 {i+1}건")

                except Exception as e:
                    self.c_log.info(f"[{i}] {products_id}:\n{e}")
                    error_id.append({
                        "_index": "none_element",
                        "_id": products_id
                    })
                    self.noti_slack(products_id+ " >> 요소찾기 실패 수동업데이트 필요")
            driver.quit()
        except Exception as e:
            self.c_log.info(f"Failed to set up WebDriver: {e}")

        fail_count = 0

        try:
            success, responses = bulk(self.es, products, raise_on_error=False)
            for response in responses:
                if 'index' in response and response['index']['status'] >= 300:
                    fail_count += 1

            if fail_count > 0:
                fail_data = []
                for response in responses:
                    for product in products:
                        if product['_id'] == response['index']['_id']:
                            fail_data.append({
                                "_index": self.es_config['fail_index'],
                                "_id": product['_id'],
                                "_source": product['_source']
                            })

                bulk(self.es, fail_data, raise_on_error=False)
                bulk(self.es, error_id, raise_on_error=False)

            self.noti_slack(f"성공 {success}건\n실패 {fail_count}건\n누락{len(error_id)}")

        except Exception as e:
            self.c_log.error(f"An unexpected error occurred: {e}")
            self.noti_slack(f"Error 크롤링 중단\n{e}")

        self.c_log.info(">>> 크롤링 완료")
        self.noti_slack(f"{time.asctime()}\n크롤링 완료")


    def crawling_retry(self):
        self.c_log.info(">>> 실패 데이터")
        self.noti_slack(f"{time.asctime()}\n실패데이터 재적재")

        try:

            while True:
                query = {
                    "query": {
                        "match_all": {}
                    }
                }
                get_data = self.es.search(
                    index=self.es_config['fail_index'], body=query)
                datas = get_data["hits"]["hits"]

                set_data = []
                for hit in datas:
                    data = {
                        "_op_type": "index",
                        "_index": self.es_config['main_index'],
                        "_id": hit["_id"],
                        "_source": hit["_source"]
                    }
                    set_data.append(data)

                get_data = []
                success_id = []
                fail_count = 0
                success, responses = bulk(
                    self.es, set_data, raise_on_error=False)
                if len(responses) > 0:
                    for data in datas:
                        for response in responses:
                            if 'index' in response and response['index']['status'] >= 300:
                                fail_count += 1
                                if data["_index"] == response['index']:
                                    temp = {
                                        "_op_type": "index",
                                        "_index": self.es_config['fail_index'],
                                        "_id": data["_id"],
                                        "_source": data["_source"]
                                    }
                                    get_data.append(temp)
                            else:
                                temp = {
                                    '_op_type': 'delete',
                                    "_index": self.es_config['fail_index'],
                                    "_id": response['index']['_id']
                                }
                                success_id.append(temp)
                else:
                    if len(get_data) == 0:
                        for data in set_data:
                            temp = {
                                '_op_type': 'delete',
                                "_index": self.es_config['fail_index'],
                                "_id": data["_id"]
                            }
                            success_id.append(temp)
                    else:
                        for gdata, sdata in zip(get_data, set_data):
                            if gdata["_id"] != sdata["_id"]:
                                temp = {
                                    '_op_type': 'delete',
                                    "_index": self.es_config['fail_index'],
                                    "_id": sdata["_id"]
                                }
                            success_id.append(temp)

                bulk(self.es, get_data, raise_on_error=False)
                bulk(self.es, success_id, raise_on_error=False)

                if len(datas) == 0:
                    self.noti_slack("크롤링 재시도 알림\n데이터가 없습니다.")
                    break

            self.c_log.info(">>> 재시도 완료")
            self.noti_slack("크롤링 재시도 성공")

        except NotFoundError:
            self.c_log.info(str(NotFoundError))

    def noti_slack(self, text):
        noti = {
            "channel": self.slack_config["channel"],
            "text": text
        }
        self.slack(noti)

    def slack(self, noti):
        response = requests.post(self.slack_config['url'], headers=self.headers, json=noti)
        if response.status_code != 200:
            self.c_log.info(f"Failed to send Slack notification. Status code: {response.status_code}, Response: {response.text}")
        else:
            self.c_log.info(
                f"Slack notification sent successfully. Response: {response.text}")
            
    def moreBtn(self, driver):
        self.c_log.info("더보기버튼 실행")
        while True:
            try:
                more_button = driver.find_element(By.CSS_SELECTOR, 'button.more_btn')
                if more_button:
                    driver.execute_script("arguments[0].click();", more_button)
                    time.sleep(2)
                else:
                    break
            except Exception as e:
                self.c_log.info(f"더보기에러: {e}")
                try:
                    alert = Alert(driver)
                    alert.accept()
                except:
                    break


    def main(self):
        self.c_log.info("===== START main() =====")

        schedule.every().day.at("18:30").do(self.crawling_main)
        schedule.every().day.at("20:00").do(self.crawling_retry)

        while True:
            schedule.run_pending()
            time.sleep(5)


if __name__ == "__main__":
    crawler = Crawler()
    crawler.main()
