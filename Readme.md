To start the code please run the following command to set up Sample data and Docker Compose.
```sh
./entrypoint.sh
```

For the first time, please run *"setup_table_dag"* inside Airflow.
Here's the location to access Airflow
```sh
localhost:8080
```
with following login information:
- Username: airflow
- Password: airflow

Here's the list of table.
- product
- sensor
- product_sensor

To run data pipeline, please run *"data_pipeline"* DAG.

To check the data quality please run *"unit_test_dag"* DAG.

To investigate the data, you can access the database through database administration tool with following information
- URL: jdbc:postgresql://localhost:5432/database
- Username: user
- Password: password

