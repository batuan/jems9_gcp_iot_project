import os
import logging
from datetime import datetime

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
import pyarrow.csv as pv
import pyarrow.parquet as pq

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
dataset_list = ["customers", "orders", "order_items", "products"]
# dataset_url = f"https://s3.amazonaws.com/nyc-tlc/trip+data/{dataset_file}"
path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'olist_data_all')


def format_to_parquet(src_file):
    if not src_file.endswith('.csv'):
        logging.error("Can only accept source files in CSV format, for the moment")
        return
    table = pv.read_csv(src_file, parse_options=pv.ParseOptions(delimiter=';'))
    print(src_file)
    print(table)
    pq.write_table(table, src_file.replace('.csv', '.parquet'))


# NOTE: takes 20 mins, at an upload speed of 800kbps. Faster if your internet has a better upload speed
def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround

    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)
    print(os.listdir(f"{path_to_local_home}/poei_data-main/"))


default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    # "depends_on_past": False,
    "retries": 1,
}

# NOTE: DAG declaration - using a Context Manager (an implicit way)
with DAG(
    dag_id="ingest_olist_dag",
    schedule_interval="@once",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['dtc-de'],
) as dag:

    download_git_dataset_task = BashOperator(
        task_id="download_dataset_task",
        bash_command=f"curl https://github.com/batuan/poei_data/archive/refs/heads/main.zip -O -J -L && unzip poei_data-main.zip && cp -R poei_data-main {path_to_local_home}"
    )
    dataset_list = ['X46789_2018-01-22T09-17-43.409Z.csv', 'X46789_2018-01-19T09-38-02.634Z.csv', 'X46789_2018-01-19T07-58-07.733Z.csv', 'X46789_2018-01-19T07-39-30.012Z.csv', 'X46789_2018-01-22T09-36-24.545Z.csv', 'X46789_2018-01-19T10-19-41.226Z.csv', 'X46789_2018-01-19T07-20-52.339Z.csv', 'X46789_2018-01-22T10-13-39.943Z.csv', 'X46789_2018-01-19T10-38-18.941Z.csv', 'X46789_2018-01-19T09-56-40.312Z.csv', 'X46789_2018-01-19T05-37-42.612Z.csv', 'X46789_2018-01-22T03-56-59.619Z.csv', 'X46789_2018-01-19T05-56-20.307Z.csv', 'X46789_2018-01-19T09-00-42.821Z.csv', 'X46789_2018-01-19T09-19-24.965Z.csv', 'X46789_2018-01-22T03-15-35.111Z.csv']
    for dataset in dataset_list:
        dataset_file = f"{path_to_local_home}/poei_data-main/{dataset}"
        parquet_file = dataset_file.replace('.csv', '.parquet')

        format_to_parquet_task = PythonOperator(
            task_id=f"format_to_parquet_task_{dataset}",
            python_callable=format_to_parquet,
            op_kwargs={
                "src_file": f"{dataset_file}",
            },
        )

        # TODO: Homework - research and try XCOM to communicate output values between 2 tasks/operators
        local_to_gcs_task = PythonOperator(
            task_id=f"local_to_gcs_task_{dataset}",
            python_callable=upload_to_gcs,
            op_kwargs={
                "bucket": BUCKET,
                "object_name": f"raw/{parquet_file.split('/')[-1]}",
                # "local_file": f"{path_to_local_home}/{dataset_file}",
                "local_file": f"{parquet_file}",
            },
        )

        delete_local_data = BashOperator(
            task_id=f"delete_local_{parquet_file.split('/')[-1]}",
            bash_command=f"rm {parquet_file} && rm {dataset_file}"
        )


        download_git_dataset_task >> format_to_parquet_task >> local_to_gcs_task >> delete_local_data
