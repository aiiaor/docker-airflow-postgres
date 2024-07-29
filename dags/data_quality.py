import psycopg2
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import os

def get_connection():
    conn = psycopg2.connect(f"dbname={os.environ['POSTGRES_DB']} user={os.environ['POSTGRES_USER']} password={os.environ['POSTGRES_PASSWORD']} host={os.environ['POSTGRES_HOST']}")
    return conn

def check_product_table():
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM product WHERE product_name IS NOT NULL")
    records = cursor.fetchone()
    if records[0] == 0:
        raise ValueError("Product table check failed: product_name is null")
    cursor.close()
    conn.close()

def check_sensor_table():
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM sensor WHERE id IS NOT NULL")
    records = cursor.fetchone()
    if records[0] == 0:
        raise ValueError("Sensor table check failed: id is null")
    cursor.close()
    conn.close()

def check_product_sensor_table():
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT COUNT(*) 
        FROM product_sensor ps
        LEFT JOIN product p ON ps.product_id = p.product_name
        LEFT JOIN sensor s ON ps.sensor_id = s.id
        WHERE p.product_name IS NULL OR s.id IS NULL
    """)
    records = cursor.fetchone()
    if records[0] > 0:
        raise ValueError("Product_Sensor table check failed: Foreign key constraint violation")
    cursor.close()
    conn.close()

with DAG(
    'unit_test_dag',
    default_args={
        'owner': 'airflow',
        'start_date': days_ago(1),
    },
    schedule_interval=None,
    catchup=False,
) as dag:

    check_product = PythonOperator(
        task_id='check_product_table',
        python_callable=check_product_table
    )

    check_sensor = PythonOperator(
        task_id='check_sensor_table',
        python_callable=check_sensor_table
    )

    check_product_sensor = PythonOperator(
        task_id='check_product_sensor_table',
        python_callable=check_product_sensor_table
    )

    check_product >> check_sensor >> check_product_sensor