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
    추출 시 condition에는 1. 기자(홍길동) 2. 날짜(2023-03-28) 3. 날짜 between(2023-03-03 2023-03-28)
                         4. (플랫폼, 대분류) 5. (플랫폼, 대분류, 소분류) 
    """

    def __init__(self, configs) -> None:
        """
        데이터베이스 접속
        인자 : 데이터베이스 접속정보
        """
        
        try:
            self.connection = pymysql.connect(**configs)
            print('데이터베이스 연결 성공!')
        except pymysql.err.OperationalError as e : 
                print('데이터베이스 연결 실패..:', e)
            
        
        tmp = [l.rstrip().split(',') for l in open('./main_category').readlines()]
        self.MAIN_CATEGORY_DICT = {v: k for k, v in tmp}
        tmp = [l.rstrip().split(',') for l in open('./sub_category').readlines()]
        self.SUB_CATEGORY_DICT = {v: k for k, v in tmp}
        tmp = [l.rstrip().split(',') for l in open('./platform_info').readlines()]
        self.PLATFORM_DICT = {v: k for k, v in tmp}

    def __del__(self) -> None:
        """
        데이터베이스 연결 해제
        """
        self.connection.close()

    def clean_title(self, title):
        pass
    
    def clean_content(self, content):
        pass

    def insert_news(self, df):
        """
        인자 : 데이터프레임
        
        우선 데이터프레임의 column명 체크하여 News 테이블의 칼럼이름과 일치하지 않을 경우 에러 발생시키기

        제목, 내용 부분 cleansing (클렌징기 하나 선정한 이후 함수로 만들어놓을 것임.)
        함수이름 clean_title(), clean_content() - 인자는 문자열

        insert SQL문 생성
        execute 대신 execute_many 메서드로 한번에 삽입 (서버 DB 1GB 램 고려)
        """
        
        # if 'url' not in list(df.columns):
        #     df['url'] = ""

        table_columns = ['main_category', 'sub_category', 'content', 'platform', 'title', 'writed_at', 'writer']
        
        df.fillna('', inplace=True) #None 값 공백으로 변환
        
        required_columns = set(table_columns) - set(df.columns)
        assert not required_columns, '테이블 칼럼 수 부족!'
        
        #df['title'] = df['title'].apply(clean_title) #cleansing
        #df['content'] = df['content'].apply(clean_content) #cleansing
        

        #Insert SQL문
        with self.connection.cursor() as cursor:
            
            #뉴스 테이블만  ##다른 테이블은 한번 따로 넣어주는 것이 좋겠다고 판단(파일 갖고 있으니)
 
            sql = "INSERT INTO `news`(`main_id`, `sub_id`, `platform_id`, `title`, `writer`, `content`, `writed_at`) VALUES (%s, %s, %s, %s, %s, %s, %s)"
            mylist=[]
            idx = 0
            
            for index, row in df.iterrows(): #데이터프레임 한 행씩 가져옴
                        
                row['main_id'] = self.MAIN_CATEGORY_DICT[row['main_category']]
                row['sub_id'] = self.SUB_CATEGORY_DICT[row['sub_category']]
                row['platform_id'] = self.PLATFORM_DICT[row['platform']]
            
                mylist.append(tuple((row['main_id'], row['sub_id'], row['platform_id'], row['title'], row['writer'], row['content'], datetime.strptime(row['writed_at'], '%Y-%m-%d %H:%M:%S'))))
                
                idx += 1 
                if idx == 10000: #데이터 10000개씩 넣기
                    cursor.executemany(sql, mylist)
                    self.connection.commit() 
                    print('모든 데이터 삽입 완료!')
                    idx = 0
                    mylist.clear()
                    
            if index != 0:
                cursor.executemany(sql, mylist) #위에서 삽입되지 못한 것들 넣기
                self.connection.commit() 
                print('남은 데이터 삽입 완료!')

            

    def select_news(self, start_date=None, end_date=None, main_category=None, sub_category=None, platform=None, writer=None): 
        """
        인자 : 데이터를 꺼내올 때 사용할 parameters

        DB에 들어있는 데이터를 꺼내올 것인데, 어떻게 꺼내올지를 고민

        인자로 받은 파라미터 별 조건을 넣은 select SQL문 작성 (1GB 램 고려)

        DB 액세스를 줄이는 방법도 한번쯤 생각해보면 좋음 (캐싱이라는 개념, *여기서 구현해야하는것은 아님)
        """
    
        with self.connection.cursor() as cursor:
            sql_pre = "SELECT * FROM `news` "
            sql_post = ""
            
            if start_date and end_date: #날짜
                sql_post += "WHERE `writed_at` BETWEEN {} AND {} ".format(start_date, end_date)
            elif start_date:
                sql_post += "WHERE `writed_at` >= {} ".format(start_date)
            elif end_date:
                sql_post += "WHERE `writed_at` <= {} ".format(end_date)
            
            if main_category: #메인 카테고리
                main_id = self.MAIN_CATEGORY_DICT[main_category]
                sql_post += "WHERE `main_id` = '{}' ".format(main_id)
            
            if sub_category: #서브 카테고리 
                sub_id = self.SUB_CATEGORY_DICT[sub_category]
                sql_post += "WHERE `sub_id` = '{}' ".format(sub_id)
            
            if platform: #플랫폼
                platform_id = self.PLATFORM_DICT[platform]
                sql_post += "WHERE `platform_id` = '{}' ".format(platform_id)
            
            if writer: #기자
                sql_post += "WHERE `writer` = '{}' ".format(writer)
                   
            sql_post += "LIMIT 10000"
            sql = sql_pre + sql_post
            cursor.execute(sql)
            results = cursor.fetchall()
            

            my_list=[]
            for result in results:
                my_list.append(list(result))

            df = pd.DataFrame(my_list, columns=[ 'news_id','title', 'writer','content',  'writed_at','url','main_category','sub_category', 'platform'])
            
            tmp = [l.rstrip().split(',') for l in open('./main_category').readlines()]
            self.MAIN_CATEGORY_DICT1 = {int(k): v for k, v in tmp}
            tmp = [l.rstrip().split(',') for l in open('./sub_category').readlines()]
            self.SUB_CATEGORY_DICT1 = {int(k): v for k, v in tmp}
            tmp = [l.rstrip().split(',') for l in open('./platform_info').readlines()]
            self.PLATFORM_DICT1 = {int(k): v for k, v in tmp}

            for index, row in df.iterrows(): #데이터프레임 한 행씩 가져옴
                row['main_category'] = self.MAIN_CATEGORY_DICT1[row['main_category']]
                df['main_category'] = row['main_category']
                row['sub_category'] = self.SUB_CATEGORY_DICT1[row['sub_category']]
                df['sub_category'] = row['sub_category']
                row['platform'] = self.PLATFORM_DICT1[row['platform']]    
                df['platform'] = row['platform']
            
               
            return df

if __name__ == '__main__':
    # 테스트코드 작성
    
    with open('./config', 'r') as f:
        lines = [line.rstrip().split('=') for line in f.readlines()]
        configs = {i: j for i, j in lines}
        configs['port'] = int(configs['port'])
    
    mydf = NewsDB() #객체 생성
    mydf.insert_news(df) #데이터베이스에 데이터 삽입
    
    test = NewsDB()
    test.select_news(start_date='2023-03-02') #2023-03-02 포함 이후 데이터 가져오기
    test.select_news(sub_category='교육') #서브 카테고리가 교육인 데이터 가져오기
