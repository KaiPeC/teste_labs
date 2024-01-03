from airflow import DAG
from datetime import datetime,timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.dummy_operator import DummyOperator
import os
cwd = os.getcwd()


seven_days_ago = datetime.combine(datetime.today() - timedelta(7),
                                    datetime.min.time())

default_args = {
        'owner': 'airflow',
        'depends_on_past': False,
        'start_date': seven_days_ago,
        'email': ['airflow@airflow.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
}

dag = DAG('yelp_dag', default_args=default_args)

start = DummyOperator(task_id="Start", dag=dag)
end = DummyOperator(task_id="End", dag=dag)

business = BashOperator(
    task_id = "business",
    bash_command=f'python3 {cwd}/processo_labs/scripts/etls/business.py',
    dag=dag
        
)

business_trusted = BashOperator(
    task_id = "business_trusted",
    bash_command=f'python3 {cwd}/processo_labs/scripts/etls/business_trusted.py',
    dag=dag
        
)

checkins= BashOperator(
    task_id = "checkins",
    bash_command=f'python3 {cwd}/processo_labs/scripts/etls/checkins.py',
    dag=dag
        
)

checkins_trusted = BashOperator(
    task_id = "checkins_trusted",
    bash_command=f'python3 {cwd}/processo_labs/scripts/etls/checkins_trusted.py',
    dag=dag
        
)

reviews = BashOperator(
    task_id = "reviews",
    bash_command=f'python3 {cwd}/processo_labs/scripts/etls/reviews.py',
    dag=dag
        
)

reviews_trusted = BashOperator(
    task_id = "reviews_trusted",
    bash_command=f'python3 {cwd}/processo_labs/scripts/etls/reviews_trusted.py',
    dag=dag
        
)

tips = BashOperator(
    task_id = "tips",
    bash_command=f'python3 {cwd}/processo_labs/scripts/etls/tips.py',
    dag=dag
        
)

tips_trusted = BashOperator(
    task_id = "tips_trusted",
    bash_command=f'python3 {cwd}/processo_labs/scripts/etls/tips_trusted.py',
    dag=dag
        
)

users= BashOperator(
    task_id = "users",
    bash_command=f'python3 {cwd}/processo_labs/scripts/etls/users.py',
    dag=dag
        
)

users_trusted = BashOperator(
    task_id = "users_trusted",
    bash_command=f'python3 {cwd}/processo_labs/scripts/etls/users_trusted.py',
    dag=dag
        
)

refined_sql = BashOperator(
    task_id = "refined_sql",
    bash_command=f'python3 {cwd}/processo_labs/scripts/etls/joins_to_mysql.py',
    dag=dag
        
)

(start 
 >> [business,
     users,
     checkins,
     tips,
     reviews] 
     >> [business_trusted,
         users_trusted,
         checkins_trusted,
         tips_trusted,
         reviews_trusted] 
         >> refined_sql
            >> end )
