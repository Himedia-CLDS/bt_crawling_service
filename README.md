### bt-crawling-service
가상환경 Virtualenv 사용해 파이썬 스크립트실행, 크롤링작업

## 파이썬 실행환경설정
# ssl설정
```
cd 파이썬 디렉토리
sudo ./configure --with-openssl=/usr/local/ssl
sudo make altinstall
```
```shell
# 가상환경 생성
python -m pip install --no-user virtualenv
virtualenv venv
python3 -m virtualenv venv --python=3.12.2

# 가상환경 활성화
.\venv\Scripts\Activate   윈도우
source venv/bin/activate  리눅스
# 활성화 안될경우 정책변경
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser

# 패키지 설치
pip install -r requirements.txt   

# 실행
python kihay.py

# 가상환경 비활성화
deactivate
```

## config.yml 작성
```shell
es:
  es_url: "http://:9200/"
  username: ""
  password: ""
  main_index: ""
  fail_index: ""

slack:
  url: ""
  channel: ""

kihay:
  url: ""

logpath:
  ec2: ""
  local: ""

driverpach:
  ec2: ""
  local: ""
```
