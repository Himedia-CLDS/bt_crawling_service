## 파이썬 실행환경설정
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