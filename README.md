# Problem Statement
[This dataset from POEI IOT project](https://github.com/batuan/poei_data/archive/refs/heads/main.zip) 
We think that this dataset contains the data of a car equipped with a variety of sensors.

In this project, we will build a data pipeline to ingest and process the data, and eventually visualise the data in Google Bigquery.


# Dashboard


# Running the project
Setup

Using Terraform to setup GCP resources
```bash
cd terraform
terraform init
terraform plan
terraform apply
terraform destroy (to remve resources once done)
```

Running Airflow on folder path
```bash
docker-compose up
```


### Data ingestion by DAG
Run the following DAGs in sequence to generate the necessary tables

1. ingest_olist_dag - Download data from source and upload to GCS

# Data Transformation using model and create new table/view in Bigquery 
- dbt 
```bash
dbt run
dbt run --full-refresh
dbt test
```

# Technologies used
- Terraform
- Docker
- Airflow
- Google Cloud Storage 
- DataFlow
- Google Bigquery
- Dbt
- Google Looker Studio

# Project Architecture
![Alt Text](https://github.com/batuan/jems9_gcp_iot_project/raw/main/IoT_project_Tech_Archi.drawio.png)
