from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import requests
import pandas as pd
from sqlalchemy import create_engine
import pymysql

# 定义默认参数
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 7, 8),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# 定义 DAG
dag = DAG(
    'api_bwibbu_all',
    default_args=default_args,
    description='An example DAG to fetch data from an API',
    schedule_interval=timedelta(days=1),  # 每天执行一次
)

# 定义 Python 函数来执行 API 请求任务
def get_api_datas():
    # API URL
    url = "https://openapi.twse.com.tw/v1/exchangeReport/BWIBBU_ALL"
    # 检查请求是否成功
    response = requests.get(url)
    if response.status_code == 200:
        # 将响应内容转换为正确的编码格式
        response.encoding = 'utf-8'
        content = response.json()
        
        # 将 JSON 转换为 DataFrame
        df = pd.DataFrame(content)
        
        # 打印 DataFrame
        print(df)
        return df.to_json()  # 转换为 JSON 字符串
    else:
        print(f"下载失败，状态码: {response.status_code}")

def df_to_csv(**kwargs):
    df_json = kwargs['task_instance'].xcom_pull(task_ids='fetch_api_data')
    df = pd.read_json(df_json)
    df.to_csv("/opt/airflow/dags/csv/bwibbu_all.csv", index=False, encoding='utf-8')
    print("DataFrame 已保存为 bwibbu_all.csv")
    
host = 'mysql'  # MySQL 主机地址
port = 3306  # MySQL 端口号
database = 'bi_stock_analysis'  # 数据库名
user = 'root'  # MySQL 用户名
password = 'root'  # MySQL 密码
table_name = 'company_ratio'  # 要写入的表名
    
# 创建 MySQL 连接引擎
def write_df_to_mysql(**kwargs):
    """
    将 DataFrame 写入 MySQL 数据库的函数

    Parameters:
    - kwargs: Airflow 提供的上下文信息，包括任务实例等

    """
    df_json = kwargs['task_instance'].xcom_pull(task_ids='fetch_api_data')
    df = pd.read_json(df_json)
    
    # 创建 MySQL 连接引擎
    engine = create_engine(f'mysql+pymysql://{user}:{password}@{host}:{port}/{database}')

    try:
        # 将 DataFrame 写入 MySQL
        df.to_sql(table_name, con=engine, if_exists='replace', index=False)
        print(f"DataFrame 已成功写入 MySQL 数据库中的表 {table_name}")
    except Exception as e:
        print(f"写入数据库时发生错误: {str(e)}")
    finally:
        # 关闭数据库连接
        engine.dispose()
    

# 定义 PythonOperator 来执行 API 请求任务
fetch_api_data = PythonOperator(
    task_id='fetch_api_data',
    python_callable=get_api_datas,
    dag=dag,
)

save_to_csv = PythonOperator(
    task_id='save_to_csv',
    python_callable=df_to_csv,
    provide_context=True,  # 需要提供上下文
    dag=dag,
)

write_to_db = PythonOperator(
    task_id='write_to_db',
    python_callable=write_df_to_mysql,
    provide_context=True,  # 需要提供上下文
    dag=dag,
)

# 设置任务依赖关系
fetch_api_data >> save_to_csv >> write_to_db