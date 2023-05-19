#!/usr/bin/env python
# coding: utf-8

# In[14]:


import pandas as pd
import random
import datetime
import threading
import time
import json
import paho.mqtt.client as mqtt #library de python

# Connect to the MQTT broker
client = mqtt.Client()

if client.connect("0.0.0.0", 1883, 60)!=0:
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

# Publish the message to the "vehicle/sensor_data" topic

header = "dateHour;gpsSpeed;gpsSatCount;Gear;Brake_pedal;Accel_pedal;Machine_Speed_Mesured;AST_Direction;Ast_HPMB1_Pressure_bar;Ast_HPMA_Pressure_bar;Pressure_HighPressureReturn;Pressure_HighPressure;Oil_Temperature;Ast_FrontAxleSpeed_Rpm;Pump_Speed"

row_count = 0
file_count = 0
max_rows_per_file = 10
data_buffer = []
start_time = time.time()

def rand():
    dateHour = datetime.datetime.now()
    gpsSpeed = random.uniform(0, 87.06)
    gpsSatCount	= random.randint(0, 255)
    Gear = random.randint(124, 137)
    Brake_pedal	= Gear
    Accel_pedal	= random.randint(0, 100)
    Machine_Speed_Mesured = 20
    AST_Direction = 20
    Ast_HPMB1_Pressure_bar = random.randint(0, 13)
    Ast_HPMA_Pressure_bar = random.randint(0, 13)
    Pressure_HighPressureReturn	= random.randint(0, 13)
    Pressure_HighPressure = random.randint(32767, 33195)
    Oil_Temperature	= random.randint(11, 255)
    Ast_FrontAxleSpeed_Rpm = random.randint(32767, 33195) # random.randit(min, max)
    Pump_Speed = random.randint(577, 1799)

    message = {
    'dateHour' : str(datetime.datetime.now()),
    'gpsSpeed' : gpsSpeed,
    'gpsSatCount'	: gpsSatCount,
    'Gear' : Gear,
    'Brake_pedal' : Gear,
    'Accel_pedal' : Accel_pedal,
    'Machine_Speed_Mesured' : 20,
    'AST_Direction' : 20,
    'Ast_HPMB1_Pressure_bar' : Ast_HPMB1_Pressure_bar,
    'Ast_HPMA_Pressure_bar' : Ast_HPMA_Pressure_bar,
    'Pressure_HighPressureReturn' : Pressure_HighPressureReturn,
    'Pressure_HighPressure' : Pressure_HighPressure,
    'Oil_Temperature' : Oil_Temperature,
    'Ast_FrontAxleSpeed_Rpm' : Ast_FrontAxleSpeed_Rpm,
    'Pump_Speed' : Pump_Speed,
    }

    return (str(dateHour) + ';' + str(gpsSpeed) + ';' + str(gpsSatCount) + ';' + str(Gear) + ';' + str(Brake_pedal) + 
            ';' + str(Accel_pedal) + ';' + str(Machine_Speed_Mesured) + ';' + str(AST_Direction) + ';' +
            str(Ast_HPMB1_Pressure_bar) + ';' + str(Ast_HPMA_Pressure_bar) + ';' + 
            str(Pressure_HighPressureReturn) + ';' + str(Pressure_HighPressure) + ';' + 
            str(Oil_Temperature) + ';' + str(Ast_FrontAxleSpeed_Rpm) + ';' + 
            str(Pump_Speed)), message

def save_data():
    global file_count, data_buffer
    if data_buffer:
        file_count += 1
        current = datetime.datetime.now()
        file_name = f"X467898_{str(current).replace(':', '_')}.csv"
        rows = [row.split(';') for row in data_buffer]
        df = pd.DataFrame(rows, columns=header.split(';'))
        # df.to_csv(file_name, index=False)
        print(f"Saved {len(data_buffer)} rows to {file_name}")
        data_buffer = []
    threading.Timer(60, save_data).start()



def printit():
    global row_count, data_buffer, start_time
    current = datetime.datetime.now()
    row_count += 1
    data, message = rand()
    # print(message)
    client.publish("vehicle/sensor_data", json.dumps(message))
    data_buffer.append(data)
    print(data)

    if row_count % max_rows_per_file == 0:
        save_data()
    if time.time() - start_time > 30000:
        print("Stopping threads...")
        threading.Timer(1, print, args=("Stopping threads...",)).start()
        for t in threading.enumerate():
            if t != threading.current_thread():
                t.cancel()
    else:
        threading.Timer(0.1, printit).start() # remember to set 0.1 in real time, 2 for debugging

save_data()
printit()


# In[ ]:



