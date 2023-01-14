import json
import pathlib
import os
import urllib.request

from airflow import models
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
	    
from custom.hooks import BlogTitlesHook

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
    
    def _store_blog_titles(conn_id):
        logger = logging.getLogger(__name__)
        
        now = datetime.now()
        formatted_date = now.strftime("%Y%m%d_%H%M%S")
        blog_titles_hook=BlogTitlesHook(conn_id=conn_id)
        title_list = blog_titles_hook.get_blog_titles(query="서울 맛집")
    
        output_dir='/home/evan/projects-personal/naver-search-keyword/data'
        os.makedir(output_dir, exist_ok=True)

        with open(f"{output_dir}/blog_titles_{formatted_date}.json", 'w', encoding='utf-8') as f:
            json.dump(title_list, f)
    
    t1 = PythonOperator(
            task_id='store_blog_titles',
            python_callable=_store_blog_titles,
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
