"""
### Tutorial Documentation
Documentation that goes along with the Airflow tutorial located
[here](https://airflow.apache.org/tutorial.html)
"""
# [START tutorial]
# [START import_module]
from datetime import timedelta
from textwrap import dedent
# import yfinance as yf
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
import airflow
from airflow.operators.python import PythonOperator
from airflow.operators.email_operator import EmailOperator

# Operators; we need this to operate!
from airflow.utils.dates import days_ago

# [END import_module]

# [START default_args]
# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['s707071127@gmail.com'],
    'email_on_failure': True,
    # 'email_on_retry': False,
    # 'retries': 0,
    # 'retry_delay': timedelta(minutes=5),
}

# [START instantiate_dag]
with DAG(
    dag_id='send-email',
    default_args=default_args,
    description='print a hello',
    schedule_interval='0 0 * * *',  # 每天凌晨 12:00
    start_date=days_ago(2),
    tags=['andychen'],
) as dag:

# dag_email = (
#     dag_id='send-email',
#     default_args=default_args,
#     description='print a hello',
#     schedule_interval=timedelta(hours=1),
#     start_date=days_ago(2),
#     tags=['andychen'],
# )
    # [END instantiate_dag]
    dag.doc_md = '''
    測試寄送 email 我要測試!
    
    '''
    
    email_task = EmailOperator(
        task_id = 'send_email',
        to = 'xxx@gmail.com',
        subject = 'print hello test',
        html_content = """<h3>Email Test</h3>""",
        dag = dag
    )
    email_task

    # t1, t2 and t3 are examples of tasks created by instantiating operators
    # [START basic_task]
   
    # [END basic_task]
    # [START jinja_template]
# [END tutorial]
