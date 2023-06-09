#!/usr/bin/env python
#
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""An Apache Beam streaming pipeline example.

It reads JSON encoded messages from Pub/Sub, transforms the message data, and
writes the results to BigQuery.
"""

from __future__ import annotations

import argparse
import json
import logging
import time
from typing import Any

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions
import apache_beam.transforms.window as window


# Defines the BigQuery schema for the output table.
SCHEMA = ",".join(
    [   "dateHour:TIMESTAMP",
        "sensorID:STRING",
        "gpsSpeed:FLOAT",
        "gpsSatCount:FLOAT",
        "Gear:FLOAT",
        "Brake_pedal:FLOAT",
        "Accel_pedal:FLOAT",
        "Machine_Speed_Measured:FLOAT",
        "AST_Direction:FLOAT",
        "Ast_HPMB1_Pressure_bar:FLOAT",
        "Ast_HPMA_Pressure_bar:FLOAT",
        "Pressure_HighPressureReturn:FLOAT",
        "Pressure_HighPressure:FLOAT",
        "Oil_Temperature:FLOAT",
        "Ast_FrontAxleSpeed_Rpm:FLOAT",
        "Pump_Speed:FLOAT",
    ]
)


def parse_json_message(message: str) -> dict[str, Any]:
    """Parse the input JSON message based on the schema."""
    row = json.loads(message)
    return {
        "dateHour": row.get("dateHour"),
        "sensorID": row.get("sensorID"),
        "gpsSpeed": row.get("gpsSpeed"),
        "gpsSatCount": row.get("gpsSatCount"),
        "Gear": row.get("Gear"),
        "Brake_pedal": row.get("Brake_pedal"),
        "Accel_pedal": row.get("Accel_pedal"),
        "Machine_Speed_Measured": row.get("Machine_Speed_Measured"),
        "AST_Direction": row.get("AST_Direction"),
        "Ast_HPMB1_Pressure_bar": row.get("Ast_HPMB1_Pressure_bar"),
        "Ast_HPMA_Pressure_bar": row.get("Ast_HPMA_Pressure_bar"),
        "Pressure_HighPressureReturn": row.get("Pressure_HighPressureReturn"),
        "Pressure_HighPressure": row.get("Pressure_HighPressure"),
        "Oil_Temperature": row.get("Oil_Temperature"),
        "Ast_FrontAxleSpeed_Rpm": row.get("Ast_FrontAxleSpeed_Rpm"),
        "Pump_Speed": row.get("Pump_Speed"),
    }



def run(
    custom_options,
    input_subscription: str,
    output_table: str,
    window_interval_sec: int = 60
) -> None:
    """Build and run the pipeline."""
        # Create the pipeline options
    pipeline_options = PipelineOptions(save_main_session=True, streaming=True)
    google_cloud_options = pipeline_options.view_as(GoogleCloudOptions)
    google_cloud_options.project = custom_options.project_name
    google_cloud_options.job_name = custom_options.job_name
    google_cloud_options.staging_location = custom_options.staging_location
    google_cloud_options.temp_location = custom_options.temp_location
    google_cloud_options.region = 'europe-west9'
    pipeline_options.view_as(beam.options.pipeline_options.StandardOptions).runner = 'DataflowRunner'

    with beam.Pipeline(options=pipeline_options) as pipeline:
        messages = (
            pipeline
            | "Read from Pub/Sub"
            >> beam.io.ReadFromPubSub(subscription=input_subscription)
            | "UTF-8 bytes to string" >> beam.Map(lambda msg: msg.decode("utf-8"))
            | "Fixed-size windows"
            >> beam.WindowInto(window.FixedWindows(int(window_interval_sec), 0))
            | "Parse JSON messages" >> beam.Map(parse_json_message)
            
        )

        # Output the results into BigQuery table.
        _ = messages | "Write to BigQuery" >> beam.io.WriteToBigQuery(
            output_table,
            schema=SCHEMA,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
        )


if __name__ == "__main__":
    # logging.getLogger().setLevel(logging.INFO)

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--output_table",
        help="Output BigQuery table for results specified as: "
        "PROJECT:DATASET.TABLE or DATASET.TABLE.",
        default="data-iot-poei-project:test_project.test_python_table1",
        type=str
    )
    parser.add_argument(
        "--input_subscription",
        help="Input Pub/Sub subscription of the form "
        '"projects/<PROJECT>/subscriptions/<SUBSCRIPTION>."',
        type=str,
        default="projects/data-iot-poei-project/subscriptions/test_pub"
    )
    parser.add_argument(
        "--window_interval_sec",
        type=int,
        help="Window interval in seconds for grouping incoming messages.",
        default=60
    )

    parser.add_argument(
        "--project_name",
        help="input project name",
        type=str,
        default="data-iot-poei-project"
    )

    parser.add_argument(
        "--job_name",
        type=str,
        help="dataflow name",
        default="streaming-iot-v"
    )

    parser.add_argument(
        "--staging_location",
        type=str,
        help="staging location",
        default="gs://temp-data-poei-batuan/staging/"
    )
    
    parser.add_argument(
        "--temp_location",
        type=str,
        help="temp location",
        default="gs://temp-data-poei-batuan/tmp/"
    )
  
    args, beam_args = parser.parse_known_args()
    print(args)

    run(
        input_subscription=args.input_subscription,
        output_table=args.output_table,
        window_interval_sec=args.window_interval_sec,
        custom_options=args
    )