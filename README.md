# 뉴스 크롤링 & 데이터베이스 사용 & 뉴스 요약 자동화 프로젝트

---

#### 팀 프로젝트
#### 개발 기간: 2023.03.17 - 2023.04.19

---

#### 1. 크롤링(2인)

 1) Beautiful Soup으로 크롤링 코드 짜기(개인)
 2) Scrapy로 크롤링 진행(개인+팀플)

#### 2. 데이터베이스(5인)

 1) 데이터 컬럼 통일(id, platform, main_category, sub_category, title, content, writer, writed_at, url, *writed_at 형식 : 2023-03-17 16:30:45)
 2) ERD 설계(팀플)
 ![최종 erd](https://user-images.githubusercontent.com/104770890/228133975-2cdc6d70-58da-4229-a55c-87fd8a43fec3.png)
 3) 데이터베이스에 ERD 바탕으로 테이블 생성(팀플)
 4) 데이터베이스 관리 스켈레톤 코드 생성(개인+팀플)
 ![db 스켈레톤 코드](https://user-images.githubusercontent.com/104770890/232951663-3663e3d8-4bef-41bc-9ff1-f904bc7d7f12.png)
 5) 데이터베이스에 데이터 삽입
 
 #### 3. 뉴스 요약 자동화(2인)
 
 1) 크롤링 자동화 코드
 ![크롤링](https://user-images.githubusercontent.com/104770890/232952054-15e773cf-11e6-4a7e-b62d-9f62ef3f6a1d.png)
![로그](https://user-images.githubusercontent.com/104770890/233257164-eee6b925-6f98-4250-aa76-1d17f7d0a180.png)
 2) 데이터 전처리 및 요약
 3) 메일 코드 작성
 4) Cron(job scheduler) 이용하여 크롤링 및 뉴스 요약 메일 주기적 발송
![크론탭](https://user-images.githubusercontent.com/104770890/233257095-033664b8-3ac3-437a-8cc5-44bc66236a0c.png)
