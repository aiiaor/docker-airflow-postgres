FROM apache/airflow:2.9.2
ADD docker-requirements.txt .
RUN pip install apache-airflow==${AIRFLOW_VERSION} -r docker-requirements.txt
