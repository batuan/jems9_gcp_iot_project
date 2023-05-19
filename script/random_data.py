#!/usr/bin/env python
# coding: utf-8

# In[14]:

import sys
import getopt
import pandas as pd
import random
import datetime
import threading
import time
import sched
import json
import paho.mqtt.client as mqtt
import google.cloud.storage as storage
import dotenv as dtenv
dtenv.load_dotenv()
import os
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')

token = os.environ.get("api-token")

# Connect to the MQTT broker
client = mqtt.Client()

if client.connect(os.environ.get('EMQX_SERVER_IP'), 1883, 60) != 0:
    print("could not connect to mqqt broker!")
    import sys
    sys.exit(-1)

# Create the message
message = {
    "dateHour": "2023-05-08T14:40:00",
    "gpsSpeed": 60,
    "gpsSatCount": 8,
    "Gear": "3",
    "Brake_pedal": 0.5,
    "Accel_pedal": 0.8
}
json_message = json.dumps(message)

# Publish the message to the "vehicuels" topic

header = "dateHour;gpsSpeed;gpsSatCount;Gear;Brake_pedal;Accel_pedal;Machine_Speed_Mesured;AST_Direction;Ast_HPMB1_Pressure_bar;Ast_HPMA_Pressure_bar;Pressure_HighPressureReturn;Pressure_HighPressure;Oil_Temperature;Ast_FrontAxleSpeed_Rpm;Pump_Speed; sensorID"

row_count = 0
max_rows_per_file = int(os.environ.get('max_rows_per_file'))
data_buffer = []
start_time = time.time()

def rand_data():
    Gear = random.randint(124, 137)
    rndData = {
        'dateHour': str(datetime.datetime.now()),
        'gpsSpeed': random.uniform(0, 87.06),
        'gpsSatCount': random.randint(0, 255),
        'Gear': Gear,
        'Brake_pedal': Gear,
        'Accel_pedal': random.randint(0, 100),
        'Machine_Speed_Mesured': 20,
        'AST_Direction': 20,
        'Ast_HPMB1_Pressure_bar': random.randint(0, 13),
        'Ast_HPMA_Pressure_bar': random.randint(0, 13),
        'Pressure_HighPressureReturn': random.randint(0, 13),
        'Pressure_HighPressure': random.randint(32767, 33195),
        'Oil_Temperature': random.randint(11, 255),
        'Ast_FrontAxleSpeed_Rpm': random.randint(32767, 33195),
        'Pump_Speed': random.randint(577, 1799)
    }
    return rndData


def save_data():
    global data_buffer, row_count
    if data_buffer:
        data_bf = data_buffer.copy()
        data_buffer = []
        row_count = 0
        current = datetime.datetime.now()
        rows = [row.split(';') for row in data_bf]
        df = pd.DataFrame(rows, columns=header.split(';'))
        file_name = "batchIoV_"+str(current).replace(':', '_')+".parquet"
        df.to_parquet(file_name, compression='gzip') #We save same df using Parquet
        print(f"Saved {len(data_bf)} rows to {file_name}")
        """Write and read a blob from GCS using file-like IO"""
        storage_client = storage.Client()
        bucket = storage_client.bucket("jems-iot-ali")
        blob = bucket.blob(file_name)
        #print("blob", blob)
        blob.upload_from_filename(file_name)
        if os.path.isfile(file_name):
            os.remove(file_name)


def stream_data(sensorID, message):
    message['sensorID'] = sensorID
    topic = os.environ.get("EMQX_TOPIC_NAME")
    result = client.publish(topic, json.dumps(message))
    # result: [0, 1]
    status = result[0]
    if status != 0:
        print(f"Failed to send message to topic {topic}")

def one_sensor_data(sensorID):
    global row_count, data_buffer
    rndData = rand_data()
    stream_data(sensorID, rndData)
    csv_data = ';'.join(map(str,rndData.values()))
    data_buffer.append(csv_data)
    row_count+=1
    

def main(argv):
    nbSensors = 1
    sensorSuffix = 's_'
    opts, args = getopt.getopt(argv, "hn:s:", ["nb=", "suffix="])
    for opt, arg in opts:
        if opt == '-h':
            print('random_data.py -n numberOfSensors -s suffixName')
            sys.exit()
        elif opt in ("-n", "--nb"):
            nbSensors = int(arg)
        elif opt in ("-s", "--suffix"):
            sensorSuffix = arg
    print('nbSensors is ', nbSensors)
    print('sensorSuffix is ', sensorSuffix)
    my_scheduler = sched.scheduler(time.time, time.sleep)
    my_scheduler.enter(0.1, 1, do_loop, (my_scheduler,nbSensors, sensorSuffix))
    my_scheduler.run()

def do_loop(scheduler, nbSensors, sensorSuffix): 
    # schedule the next call first
    scheduler.enter(0.1, 1, do_loop, (scheduler,nbSensors, sensorSuffix))
    # then do your stuff
    #print('row_count=', row_count)
    threads = []
    start = time.perf_counter()
    for i in range(nbSensors):
        sensorID = sensorSuffix+str(i)
        t = threading.Thread(target=one_sensor_data(sensorID), args=(sensorID,))
        t.start()
        threads.append(t)
    for thread in threads:
        thread.join()
    if row_count % max_rows_per_file == 0:
        save_data()
    finish = time.perf_counter()
    #print(f'Finished in {round(finish-start, 2)} second(s)')

if __name__ == "__main__":
    main(sys.argv[1:])

# In[ ]:



