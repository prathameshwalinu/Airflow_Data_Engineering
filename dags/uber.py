from airflow.decorators import dag, task
from datetime import datetime
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator
from astro import sql as aql
from astro.files import File
from astro.sql.table import Table, Metadata
from astro.constants import FileType

@dag(
    start_date=datetime(2023,1,1),
    schedule=None,
    catchup=False,
    tags=['uber'],
)

def uber():
    
    upload_csv_to_gcs = LocalFilesystemToGCSOperator(
        task_id='upload_csv_to_gcs',
        src='/usr/local/airflow/include/dataset/uber_data.csv',
        dst='raw/uber_data.csv',
        bucket='uber_data_ph',  
        gcp_conn_id='gcp',
        mime_type='text/csv',
    )

    create_uber_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id='create_uber_dataset',
        dataset_id='uber',
        gcp_conn_id='gcp',
    )

    gcs_to_raw = aql.load_file(
        task_id='gcs_to_raw',
        input_file=File(
            'gs://uber_data_ph/raw/uber_data.csv',
            conn_id='gcp',
            filetype=FileType.CSV,
        ),
        output_table=Table(
            name='uber_tlc_trip_recrord_data',
            conn_id='gcp',
            metadata=Metadata(schema='uber')
        ),
        use_native_support=False,
    )

uber()