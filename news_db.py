import pandas as pd
import pymysql
from datetime import datetime
import time

"""
사용할 라이브러리 : pymysql, pandas

요구사항
1. Docstring 작성
2. 클래스 내 생성자, 소멸자, insert, select 함수 구현
3. 테스트를 위한 실행 코드 작성
"""

class NewsDB:
    """
    생성시 데이터베이스(news_db) 접속, 소멸시 연결 해제.
    데이터 삽입 함수: insert_news(self, df)
    데이터 추출 함수: select_news(self, condition)
    추출 시 condition에는 1. 기자(홍길동) 2. 날짜(2023-03-28) 3. (플랫폼, 대분류) 4. (플랫폼, 대분류, 소분류)
    """

    def __init__(self) -> None:
        """
        데이터베이스 접속
        인자 : 데이터베이스 접속정보
        """
        global connection
        connection = pymysql.connect()

    def __del__(self) -> None:
        """
        데이터베이스 연결 해제
        """
        connection.close()


    def insert_news(self, df):
        """
        인자 : 데이터프레임
        
        우선 데이터프레임의 column명 체크하여 News 테이블의 칼럼이름과 일치하지 않을 경우 에러 발생시키기

        제목, 내용 부분 cleansing (클렌징기 하나 선정한 이후 함수로 만들어놓을 것임.)
        함수이름 clean_title(), clean_content() - 인자는 문자열

        insert SQL문 생성
        execute 대신 execute_many 메서드로 한번에 삽입 (서버 DB 1GB 램 고려)
        """
        
        df_cols = list(df.columns) 
        
        with connection.cursor() as cursor:
            cols = cursor.fetchall()
            cols_list = []
            for col in cols:
                cols_list.append(col[0])
                

            if sorted(df_cols) != sorted(cols_list):
                raise ValueError #에러 발생

                       
        titles = []
        for title in df['title']:
            new_title = clean_title(title) #cleansing
            titles.append(new_title)
        df['title'] = titles
        
        contents = []
        for content in df['content']:
            new_content = clean_content(content) #cleansing
            contents.append(new_content)
        df['content'] = contents
        
        
        #insert SQL문
        with connection.cursor() as cursor:
            
            #뉴스 테이블
            sql = "INSERT INTO `news`(`news_id`, `title`, `writer`, `content`, `writed_at`, `url`) VALUES(%s, %s, %s, %s, %s, %s)"
            mylist=[]
            
            for index, row in df.iterrows(): #데이터프레임 한 행씩 가져옴
                mylist.append(tuple((int(row[0]), row[1], row[2], row[3], datetime.strptime(row[4], '%Y-%m-%d %H:%M:%S'), row[5])))
            
            cursor.executemany(sql, mylist)
            
            #서브카테고리 테이블
            sql = "INSERT INTO `sub_category`(`sub_id`, `name`) VALUES(%s, %s)"
            mylist=[]
            
            for index, row in df.iterrows(): #데이터프레임 한 행씩 가져옴
                mylist.append(tuple((int(row[0]), row[1])))
            
            cursor.executemany(sql, mylist)
            
            #메인카테고리 테이블
            sql = "INSERT INTO `main_category`(`main_id`, `name`) VALUES(%s, %s)"
            mylist=[]
            
            for index, row in df.iterrows(): #데이터프레임 한 행씩 가져옴
                mylist.append(tuple((int(row[0]), row[1])))
            
            cursor.executemany(sql, mylist)
            
            #플랫폼 테이블
            sql = "INSERT INTO `platform_info`(`platform_id`, `name`) VALUES(%s, %s)"
            mylist=[]
            
            for index, row in df.iterrows(): #데이터프레임 한 행씩 가져옴
                mylist.append(tuple((int(row[0]), row[1])))
            
            cursor.executemany(sql, mylist)
            
            
            connection.commit() 


    def select_news(self, condition): 
        """
        인자 : 데이터를 꺼내올 때 사용할 parameters

        DB에 들어있는 데이터를 꺼내올 것인데, 어떻게 꺼내올지를 고민

        인자로 받은 파라미터 별 조건을 넣은 select SQL문 작성 (1GB 램 고려)

        DB 액세스를 줄이는 방법도 한번쯤 생각해보면 좋음 (캐싱이라는 개념, *여기서 구현해야하는것은 아님)
        """
        with connection.cursor() as cursor:
            sql_pre = "SELECT DISTINCT * FROM `news`"
            sql_post = ""
            
            cons = condition.split() #플랫폼, 대분류 (네이버 사회)
            if cons[1] in ['정치','경제','사회','세계','IT/과학','스포츠','생활/문화','IT','문화','국제']:
                
                sql_post = """ 
                INNER JOIN `platform_info` 
                ON news.platform_id = platform_info.platform_id 
                WHERE `name` = '{}'
                INNER JOIN `main_category`
                On news.main_id = main_category.main_id
                WHERE `name` = '{}' 
                """.format(cons[0], cons[1]) 
            
            cons = condition.split() #플랫폼, 대분류, 소분류 (네이버 사회 사건사고)
            if cons[1] in ['정치','경제','사회','세계','IT/과학','스포츠','생활/문화','IT','문화','국제']:
                
                sql_post = """ 
                INNER JOIN `platform_info` 
                ON news.platform_id = platform_info.platform_id 
                WHERE `name` = '{}'
                INNER JOIN `main_category`
                On news.main_id = main_category.main_id
                WHERE `name` = '{}' 
                INNER JOIN `sub_category`
                On news.sub_id = sub_category.sub_id
                WHERE `name` = '{}' 
                """.format(cons[0], cons[1], cons[2]) 
                
            if (cons[0] != '네이버') and (cons[0] != '다음'): #기자 이름 (홍길동)
                sql_post = """
                WHERE `writer` = '{}'
                """.format(cons[0])
            
            if (len(cons[0]) == 10): #날짜 (2023-03-28)
                sql_post = """
                WHERE `writed_at` = '{}'
                """.format(cons[0])

            sql = sql_pre + sql_post
            cursor.execute(sql)
            result = cursor.fetchall()
            print(result)
            return result

if __name__ == '__main__':
    # 테스트코드 작성
    
    mydf = NewsDB() #객체 생성
    mydf.insert_news(df) #데이터베이스에 데이터 삽입
    
    naver_social = NewsDB()
    naver_social.select_news('네이버 사회') #네이버 사회 데이터 가져오기 및 출력
    
    naver_social_사건사고 = NewsDB()
    naver_social_사건사고.select_news('네이버 사회 사건사고') #네이버 사회 데이터 가져오기 및 출력
    
    홍길동 = NewsDB()
    홍길동.select_news('홍길동') #홍길동 기자 데이터 가져오기 및 출력
    
    one_date = NewsDB()
    one_date.select_news('2023-03-28') #날짜 데이터 가져오기 및 출력
    
    
