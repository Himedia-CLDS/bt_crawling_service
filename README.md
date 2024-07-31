## 파이썬 실행환경설정
```shell
# 가상환경 생성
python -m venv venv       

# 가상환경 활성화
.\venv\Scripts\Activate   윈도우
source venv/bin/activate  리눅스
# 가상환경 비활성화ㅂ
deactivate

# 패키지 설치
pip install -r requirements.txt   
```

## security.json 작성
```shell
"es" : {
    "es_url": "",
    "username": "",
    "password": ""
}
"slack" : {
    "url" : "",
    "chnnel" : ""
},
"kihay": {
    "url" : ""
}
```