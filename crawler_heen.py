#필요 모듈 임포트
import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
from .cleansing import cleansing

class Crawler:

    def __init__(self) -> None:
        # 마지막 뉴스 url 가져오기
        with open('./naver_news_url', 'r') as f:
            self.last_news_url = f.read().strip()

        # 다음뉴스
        pass

    def naver_crawler(self):
        
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36'}

        sub_category_num = [731,226,227,230,732,283,229,228] #IT/과학 서브카테고리 

        news_list = []
        for sub_num in sub_category_num:

            URL = "https://news.naver.com/main/list.naver?mode=LS2D&mid=shm&sid2={}&sid1=105".format(sub_num) #IT/과학

            #총 페이지 수 구하기
            URL = URL + "&page={}"
            page = 1

            while True:
                response = requests.get(URL.format(page), headers=headers)    
                soup = BeautifulSoup(response.text, 'html.parser')
                try: 
                    if soup.select_one('.next').text == '다음':
                        page += 1
                except:
                    break
                
            #date는 와일문 돌려서 최근기사부터 시작. last url 저장되어있는 날짜까지...
            while True:
                
                if page > 2:
                    for_pages = soup.find(class_="paging" )
                    pages = for_pages.find_all('a')
                    plus_page = len(pages) - 1
                else:
                    for_pages = soup.find(class_="paging" )
                    pages = for_pages.find_all('a')
                    plus_page = len(pages)

                total_page = page + plus_page

                for page in range(1,total_page+1):

                    response = requests.get(URL.format(page), headers=headers)    
                    soup = BeautifulSoup(response.text, 'html.parser')

                    url_in = soup.select('div[id="main_content"] li') #한 페이지 최대 20개
                    url_list = []
                    for li in url_in:
                        url_one = li.select_one('dt a[href]').attrs['href'] #기자와 작성시간 알기 위해 기사 링크 추가
                        url_list.append(url_one)

                    #크롤링 할 때 젤 먼저 저장한 url이 가장 최신 뉴스 (옛날 뉴스가 아래로 쌓이니까)
                    #그럼 어떻게 해야하지../??

                    for url in url_list: #한 링크씩 들어가기

                        url = url
                        response = requests.get(url, headers=headers)
                        time.sleep(1.5)
                        soup = BeautifulSoup(response.text, 'html.parser')
                        
                        news_url = url
                        if news_url == self.last_news_url: #최신뉴스까지만!
                            print('최신까지 크롤링 완료')
                            with open('./naver_news_url', 'w') as f: #마지막 url 저장
                                f.write(url)
                            #return news_list #크롤링 종료
                            break
                        
                        news_dict = {}
                        news_dict['platform'] = '네이버'
                        news_dict['main_category'] = 'IT/과학'
                        
                        sub_name = [l.rstrip().split(',') for l in open('./sub_category').readlines()]
                        SUB_CATEGORY_DICT = {v: k for k, v in sub_name}
                        
                        sub_name = SUB_CATEGORY_DICT[sub_num]
                        news_dict['sub_category'] = '{}'.format(sub_name)

                        try:
                            news_dict['title'] = soup.select_one('.media_end_head_headline').text.strip()
                        except:
                            try:
                                news_dict['title'] = soup.select_one('.media_end_head_headline').text.strip()
                            except:
                                try:
                                    news_dict['title'] = soup.select_one('h2[id="title_area"]').text.strip()
                                except:
                                    news_dict['title'] = soup.select_one('.title').text.strip()
                        try:
                            news_dict['content'] = soup.select_one('div[id="newsct_article"]').text.strip()
                        except:
                            try:
                                news_dict['content'] = soup.select_one('div[id="dic_area"]').text.strip()
                            except:
                                try:
                                    news_dict['content'] = soup.select_one('div[id="articeBody"]').text.strip()
                                except:
                                    news_dict['content'] = soup.select_one('.news_end').text.strip()
                        try:
                            news_dict['writer'] = soup.select_one('.media_end_head_journalist_name').text.strip()
                        except:
                            try:
                                news_dict['writer'] = soup.select_one('.byline_s').text[:6]
                            except:
                                try:
                                    news_dict['writer'] = soup.select_one('.byline').text[:3].strip()
                                except:
                                    news_dict['writer'] = None
                        try:
                            news_dict['writed_at'] = soup.select_one('.media_end_head_info_datestamp_time').text.strip()
                        except:
                            try:
                                news_dict['writed_at'] = soup.select_one('.author em').text.strip()
                            except:
                                news_dict['writed_at'] = soup.select_one('.info').text.strip()

                        news_list.append(news_dict)
                        
    
    def processing(self, news_list):                    
        df = pd.DataFrame([news_list[0]])
        for news in news_list[1:]:
            df = df.append([news], ignore_index=True)
        
        ##  [writed_at] YYYY-MM-DD HH:MM:SS 형식으로 변경
        df['writed_at'] = df['writed_at'].str.replace('오후', 'PM')
        df['writed_at'] = df['writed_at'].str.replace('오전', 'AM')
        df['writed_at'] = pd.to_datetime(df['writed_at'], format='%Y.%m.%d. %p %I:%M')
        df['writed_at'] = df['writed_at'].apply(lambda x : x.strftime('%Y-%m-%d %H:%M:%S') )

        ## [platform] Naver -> 네이버
        df['platform'] = df['platform'].apply(lambda x: x.replace('Naver', '네이버'))
        
        df['title'] = df['title'].apply(lambda x : x.replace('\n', ' ')[:160])
        
        df['content'] = df.apply(lambda x: cleansing(x['content'], x['writer'] if x['writer'] else ''), axis=1)
        
        df['url'] = ''
                    
        self.news_df = df
        return df


import pandas as pd
class DataManager:
    """
    데이터를 관리하는 객체
    """

    def __init__(self) -> None:
        # 기존 데이터를 읽어옴
        self.df = pd.read_csv('./news.txt')
        pass

    def add_new_data(self, new_data):
        self.df = pd.merge([self.df, new_data])
        return
    
    def save_data(self, file_path):
        self.df.to_csv(file_path)
        return


if __name__ == '__main__':
    # 객체지향 프로그래밍 -> 컴포넌트 단위로 관리
    my_crawler = Crawler()
    my_crawler.naver_crawler()
    my_crawler.news 

    my_data_manager = DataManager()
    my_data_manager.add_new_data(my_crawler.news)
    my_data_manager.save_data('./news.txt')
