from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
from tqdm import tqdm
import psycopg2
from psycopg2 import sql


import os
conn = psycopg2.connect(f"dbname={os.environ['POSTGRES_DB']} user={os.environ['POSTGRES_USER']} password={os.environ['POSTGRES_PASSWORD']} host={os.environ['POSTGRES_HOST']}")
cur = conn.cursor()

prev_sensor = pd.DataFrame()
prev_product = pd.DataFrame()
prev_sensor_product = pd.DataFrame()

def read_current_data():
    cur.execute("SELECT * FROM sensor")
    sensor = cur.fetchall()
    prev_sensor = pd.DataFrame(sensor, columns=['id', 'sensor_serial', 'department_name'])
    
    cur.execute("SELECT * FROM product")
    product = cur.fetchall()
    prev_product = pd.DataFrame(product, columns=['product_name', 'product_expire'])
    
    cur.execute("SELECT s.sensor_serial, s.department_name,ps.product_id FROM product_sensor ps JOIN sensor s ON ps.sensor_id = s.id")
    sensor_product = cur.fetchall()
    prev_sensor_product = pd.DataFrame(sensor_product, columns=['sensor_serial', 'department_name', 'product_name'])
    
    return prev_sensor, prev_product, prev_sensor_product

def load_data():

    prev_sensor,prev_product,prev_sensor_product = read_current_data()
    
    file_list = sorted(os.listdir('dags/data_sample'))
    
    data = pd.DataFrame()
    tmp_data = pd.DataFrame()
    file_counter = 0
    
    for file in tqdm(file_list):
        # Load data chunked by date
        data_file = os.path.join('dags/data_sample', file)

        if file_counter == 250:
            file_counter = 0
            tmp_data = pd.read_parquet(data_file)
            os.rename(data_file, os.path.join('dags/processed_data', file))
        else:
            file_counter += 1
            data = pd.concat([data, pd.read_parquet(data_file)])
            #move file to processed folder
            os.rename(data_file, os.path.join('dags/processed_data', file))
            continue

        print(f"Load data from {data_file}")
        print(f"Data shape: {data.shape}")
        prev_sensor,prev_product,prev_sensor_product=process_data(data, prev_sensor, prev_product, prev_sensor_product)
        data = tmp_data
        
    process_data(data,prev_sensor,prev_product,prev_sensor_product)
    cur.close()
    conn.close()
    
    print("Insert data successfully")

def process_data(data, prev_sensor, prev_product, prev_sensor_product):
        
        if len(prev_sensor) > 0:
            sensor_df = data[['sensor_serial', 'department_name']].drop_duplicates()
            merged_df = sensor_df.merge(prev_sensor, how='left', indicator=True)
            new_sensor = merged_df[merged_df['_merge'] == 'left_only'].drop(columns='_merge')
            sensor_df = pd.concat([prev_sensor, new_sensor])
        else:
            sensor_df = data[['sensor_serial', 'department_name']].drop_duplicates()
            new_sensor = sensor_df

        if len(new_sensor) > 0:
            # Construct string for query
            values = ", ".join([f"('{row['sensor_serial']}', '{row['department_name']}')" for index, row in new_sensor.iterrows()])
            insert_sensor_query = sql.SQL(f'''
                INSERT INTO sensor (sensor_serial, department_name)
                VALUES {values}
            ''')
        
            cur.execute(insert_sensor_query)

        conn.commit()

        product_df = data[['product_name', 'product_expire']].groupby('product_name').agg({'product_expire': 'max'}).reset_index()
        if len(prev_product) > 0:
            updated_product = pd.concat([prev_product, product_df]).groupby('product_name').agg({'product_expire': 'max'}).reset_index()
        else:
            updated_product = product_df

        cur.execute("TRUNCATE TABLE product;")

        # Construct string for query
        values = ", ".join([f"('{row['product_name']}', '{row['product_expire']}')" for index, row in updated_product.iterrows()])

        insert_product_query = sql.SQL(f'''
            INSERT INTO product (product_name, product_expire)
            VALUES {values}
        ''')

        cur.execute(insert_product_query)

        conn.commit()

        sensor_product_df = data[['sensor_serial', 'department_name', 'product_name']].drop_duplicates()
        if len(prev_sensor_product) > 0:
            merged_df = sensor_product_df.merge(prev_sensor_product, how='left', indicator=True)
            new_sensor_product = merged_df[merged_df['_merge'] == 'left_only'].drop(columns='_merge')
            sensor_product_df = pd.concat([prev_sensor_product, new_sensor_product]).drop_duplicates()
        else:
            new_sensor_product = sensor_product_df

        # Construct string for query
        values = ", ".join([f"('{row['sensor_serial']}', '{row['department_name']}', '{row['product_name']}')" for index, row in new_sensor_product.iterrows()])

        if len(new_sensor_product) > 0:
            insert_fact_query = sql.SQL(f'''
                INSERT INTO product_sensor (product_id, sensor_id)
                SELECT data.product_name, sensor.id
                FROM (
                VALUES 
                    {values}
                ) AS data (sensor_serial, department_name, product_name)
                LEFT JOIN sensor ON data.sensor_serial = sensor.sensor_serial AND data.department_name = sensor.department_name
            ''')

            cur.execute(insert_fact_query)

        conn.commit()
        
        prev_sensor = sensor_df
        prev_product = updated_product
        prev_sensor_product = sensor_product_df

        return prev_sensor, prev_product, prev_sensor_product

        


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 7, 11),
    'retries': 0,
}

dag = DAG('data_pipeline', default_args=default_args, schedule_interval='@daily')


insert_data = PythonOperator(
    task_id='generate_data',
    python_callable=load_data,
    dag=dag
)

insert_data


