import json
import pathlib
import os
import urllib.request

from airflow import models
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
	    
# start_date를 현재날자보다 과거로 설정하면, 
# backfill(과거 데이터를 채워넣는 액션)이 진행됨
	
default_args = {
        'owner': 'aohus',
        'depends_on_past': False,
        'start_date': datetime(2022, 12, 30),
        'email': ['dbtnghk528@gmail.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5)}
	
# dag 객체 생성
with models.DAG(
        dag_id='notice_titles_of_restaurant', 
        description='get restaurant titles from naver blog search and notice most used words', 
        schedule_interval = '0/20 * * * *', 
        default_args=default_args) as dag:
    
    def _get_blog_titles():
        client_id = "un_OV5YDP0N1dOvzs71w" #os.getenv("NAVER_CLIENT_ID")
        client_secret = "lPMIYHfp_5" #os.getenv("NAVER_SECRET")
        encText = urllib.parse.quote("서울 맛집")
        result_num = 100
        url = f"https://openapi.naver.com/v1/search/blog?query={encText}&display={result_num}&sort=date" # JSON 결과
        now = datetime.now()
        formatted_date = now.strftime("%Y%m%d_%H%M%S")
        
        try:
            request = urllib.request.Request(url)
            request.add_header("X-Naver-Client-Id",client_id)
            request.add_header("X-Naver-Client-Secret",client_secret)
            response = urllib.request.urlopen(request)
            response_body = response.read()
            result = eval(response_body.decode('utf-8'))
            file_output_path='/home/evan/projects-personal/naver-search-keyword/src/airflow/data'
            with open(f"{file_output_path}/blog_titles_{formatted_date}.json", 'w') as f:
                json.dump(result, f)
        except :
            print("Error")
    
    t1 = PythonOperator(
            task_id='get_blog_titles',
            python_callable=_get_blog_titles,
            dag=dag)
		
    # BashOperator를 사용
    # task_id는 unique한 이름이어야 함
    # bash_command는 bash에서 date를 입력한다는 뜻
		
    t2 = BashOperator(
            task_id='notify',
            bash_command='skel success',
            retries=3,
            dag=dag)
		
    t1 >> t2
