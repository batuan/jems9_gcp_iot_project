provider "google" {
#   credentials = "/Users/batuan/Documents/apply_viec_2023/jems/DataScientest/data-iot-poei-project-321b743606a5.json"
  project     = "data-iot-poei-project"
  region      = "europe-west9"
}


resource "google_pubsub_topic" "cars_sensor_topic" {
  name = "my-cars-sensor-topic"
}

resource "google_pubsub_subscription" "subscription1" {
  name   = "my-cars-sensor-subscription"
  topic  = google_pubsub_topic.cars_sensor_topic.name
  ack_deadline_seconds = 10
}


resource "google_bigquery_dataset" "cars_sensor_dataset" {
  dataset_id = "cars_sensor_dataset"
  location   = "europe-west9"
}


resource "google_bigquery_table" "cars_sensor_table" {
  dataset_id = google_bigquery_dataset.cars_sensor_dataset.dataset_id
  table_id   = "cars-sensor-real-time-data"
  
  time_partitioning {
    type = "HOUR"
    field = "dateHour"
  }
 
  schema = <<EOF
  [
  {
    "name": "dateHour",
    "mode": "NULLABLE",
    "type": "TIMESTAMP",
    "description": null,
    "fields": []
  },
  {
    "name": "gpsSpeed",
    "mode": "NULLABLE",
    "type": "FLOAT",
    "description": null,
    "fields": []
  },
  {
    "name": "gpsSatCount",
    "mode": "NULLABLE",
    "type": "INTEGER",
    "description": null,
    "fields": []
  },
  {
    "name": "Gear",
    "mode": "NULLABLE",
    "type": "INTEGER",
    "description": null,
    "fields": []
  },
  {
    "name": "Brake_pedal",
    "mode": "NULLABLE",
    "type": "INTEGER",
    "description": null,
    "fields": []
  },
  {
    "name": "Accel_pedal",
    "mode": "NULLABLE",
    "type": "INTEGER",
    "description": null,
    "fields": []
  },
  {
    "name": "Machine_Speed_Mesured",
    "mode": "NULLABLE",
    "type": "INTEGER",
    "description": null,
    "fields": []
  },
  {
    "name": "AST_Direction",
    "mode": "NULLABLE",
    "type": "INTEGER",
    "description": null,
    "fields": []
  },
  {
    "name": "Ast_HPMB1_Pressure_bar",
    "mode": "NULLABLE",
    "type": "INTEGER",
    "description": null,
    "fields": []
  },
  {
    "name": "Ast_HPMA_Pressure_bar",
    "mode": "NULLABLE",
    "type": "INTEGER",
    "description": null,
    "fields": []
  },
  {
    "name": "Pressure_HighPressureReturn",
    "mode": "NULLABLE",
    "type": "INTEGER",
    "description": null,
    "fields": []
  },
  {
    "name": "Pressure_HighPressure",
    "mode": "NULLABLE",
    "type": "INTEGER",
    "description": null,
    "fields": []
  },
  {
    "name": "Oil_Temperature",
    "mode": "NULLABLE",
    "type": "INTEGER",
    "description": null,
    "fields": []
  },
  {
    "name": "Ast_FrontAxleSpeed_Rpm",
    "mode": "NULLABLE",
    "type": "INTEGER",
    "description": null,
    "fields": []
  },
  {
    "name": "Pump_Speed",
    "mode": "NULLABLE",
    "type": "INTEGER",
    "description": null,
    "fields": []
  },
  {
    "name": "topic",
    "mode": "NULLABLE",
    "type": "STRING",
    "description": null,
    "fields": []
  },
  {
    "name": "clientid",
    "mode": "NULLABLE",
    "type": "STRING",
    "description": null,
    "fields": []
  }
]
  EOF
}


resource "google_storage_bucket" "bucket1" {
    name          = "sensor-cars-test-bucket1"
    location      = "europe-west9"
    force_destroy = true
}
resource "google_storage_bucket_object" "tmp_directory" {
  name       = "tmp/"
  bucket     = google_storage_bucket.bucket1.name
  content = " "
}


# resource "google_dataflow_job" "pubsub_stream" {
#     name = "realtime-sensors-dataflow-job1"
#     template_gcs_path = "gs://dataflow-templates/2022-01-24-00_RC00/PubSub_Subscription_to_BigQuery"
#     temp_gcs_location = google_storage_bucket_object.tmp_directory.name
#     enable_streaming_engine = true

#     parameters = {
#       inputSubscription = google_pubsub_subscription.subscription1.id
#       outputTableSpec    = google_bigquery_table.cars_sensor_table.table_id
#     }
#     on_delete = "drain"
# }

module "dataflow" {
  source  = "terraform-google-modules/dataflow/google"
  version = "2.2.0"

  project_id  = "data-iot-poei-project"
  name = "realtime-sensors-dataflow-job1"
  on_delete = "drain"
  region = "europe-west9"
  max_workers = 1
  template_gcs_path =  "gs://dataflow-templates-europe-west9/latest/PubSub_Subscription_to_BigQuery"
  temp_gcs_location = google_storage_bucket_object.tmp_directory.name
  parameters = {
        inputSubscription = "projects/data-iot-poei-project/subscriptions/my-cars-sensor-subscription"
        outputTableSpec    = "data-iot-poei-project:cars_sensor_dataset.cars-sensor-real-time-data"
  }
}