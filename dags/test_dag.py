from airflow.decorators import dag, task
from datetime import datetime
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator

@dag(
    start_date=datetime(2023,1,1),
    schedule=None,
    catchup=False,
    tags=['test_dag'],
)

def test_dag():
    upload_csv_to_gcs = LocalFilesystemToGCSOperator(
        task_id='upload_csv_to_gcs',
        src='/Users/prathamesh/Documents/GitHub/Online_Retail_Data_Engineering/Online_Retail_Data_Engineering/include/dataset/uber_data.csv',
        dst='raw/uber_data.csv',
        bucket='airflow_uber_ph',  
        gcp_conn_id='gcp',
        mime_type='text/csv',
    )
test_dag()