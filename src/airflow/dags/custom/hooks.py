from airflow.hooks.base_hook import BaseHook

import json
import os
import re

class BlogTitlesHook(BaseHook):
    def __init__(self, conn_id):
        super().__init__()
        
    def get_blog_titles(query, datetime):
        client_id = "un_OV5YDP0N1dOvzs71w" #os.getenv("NAVER_CLIENT_ID")
        client_secret = "lPMIYHfp_5" #os.getenv("NAVER_SECRET")
        encText = urllib.parse.quote(query)
        result_num = 100
        url = f"https://openapi.naver.com/v1/search/blog?query={encText}&display={result_num}&sort=date" # JSON 결과

        try:
            request = urllib.request.Request(url)
            request.add_header("X-Naver-Client-Id",client_id)
            request.add_header("X-Naver-Client-Secret",client_secret)
            response = urllib.request.urlopen(request)
            response_body = response.read()
            result = eval(response_body.decode('utf-8'))
        except :
            print("Error")

        title_list=[]
        for item in result['items']:
            title = re.sub("<b>|\\\/|<\\\/b>", "", item['title'])
            link = re.sub("\\\/","",item['link'])
            d = { 
                    'title':title,
                    'postdate':item['postdate'],
                    'link':link
                }
            title_list.append(d)
        
        return title_list
