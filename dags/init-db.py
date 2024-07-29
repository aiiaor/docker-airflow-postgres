from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime


def create_table():
    # Create table in postgresql
    import psycopg2
    conn = psycopg2.connect("dbname=database user=user password=password host=postgres-db")
    cur = conn.cursor()
    
    cur.execute("CREATE TABLE IF NOT EXISTS product (product_name VARCHAR(50) PRIMARY KEY, product_expire TIMESTAMP NOT NULL);")    

    cur.execute("CREATE TABLE IF NOT EXISTS sensor (id SERIAL PRIMARY KEY, sensor_serial VARCHAR(70) NOT NULL, department_name VARCHAR(50) NOT NULL);")

    cur.execute("CREATE TABLE IF NOT EXISTS product_sensor (product_id VARCHAR(50) NOT NULL, sensor_id INT NOT NULL);")
    
    conn.commit()
    cur.close()
    conn.close()
    print("Table created successfully")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 7, 11),
    'retries': 0,
}

dag = DAG('setup_table_dag', default_args=default_args, schedule_interval='@daily')

create_table = PythonOperator(task_id='generate_table', python_callable=create_table, dag=dag)


create_table