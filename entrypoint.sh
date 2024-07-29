#! /bin/bash

#check if data in /dags/data_sample is exist
if [ -d "dags/data_sample" ]; then
    echo "data_sample folder is exist, no need to create new one"
else
    pip install -r requirements.txt
    python3 sampledata.py
    chmod -R u+x data_sample
    mv data_sample dags/data_sample
fi

if [ -d "dags/processed_data" ]; then
    echo "data_sample folder is exist, no need to create new one"
else
    mkdir dags/processed_data
fi

#start airflow
docker-compose build
docker-compose up init-ariflow
docker-compose up