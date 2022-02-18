# Crawling G2G

 - 크롤링 대상 사이트: [G2G](https://www.g2g.com/)
 - 프로젝트: 2021-01-10 ~ 
   - 데이터 수집 코드: 2021-01-10 ~ 2021-02-01
   - 데이터 수집: 2021-02-05 ~ 
   - 
 - 1인 프로젝트
 - 주제: 게임([POE](https://poe.game.daum.net/)) 통화 현거래 데이터 수집
 - file: [crawl_URL_boost.py](https://github.com/hangack/project-green/blob/main/crawl-g2g/source/crawl_URL_boost.py)


## Requirement

 - 사용 프로그램:
   - 가상환경(python 3.10):
     - pandas 1.3.5
     - selenium 4.1.0
       - geocodriver 0.30.0-win64
       - chromedriver 97.04692.71
     - beautifulsoup4 4.10.0
     - datetime
     - time
     - re
     - psycopg2 2.9.3
     - google-api-core 2.5.0
     - pandas_gbq 0.17.0
     - pyinstaller 4.8
   - sql:
     - postgresql 13.5
     - Google Cloud Platform - BigQuery
   - nncronlt117 119
 - 개발환경:
   - windows 10 x64



## source code(python)

 - file: [crawl_URL_boost.py](https://github.com/hangack/project-green/blob/main/crawling-g2g/source/crawl_URL_boost.py)


## blog Category

 - [G2G-crwaling](https://hangack.github.io/categories/%EC%97%B0%EC%8A%B5/%ED%8C%8C%EC%9D%B4%EC%8D%AC/crwaling-G2G/)

## PPT
 - pdf: [현거래 Data 수집.pdf](https://github.com/hangack/project-green/blob/main/crawling-g2g/docs/%ED%98%84%EA%B1%B0%EB%9E%98%20Data%20%EC%88%98%EC%A7%91.pdf)