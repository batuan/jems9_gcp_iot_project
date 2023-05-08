#!/usr/bin/env python
# coding: utf-8

# In[14]:


import pandas as pd
import random
import datetime
import threading
import time

header = "dateHour;gpsSpeed;gpsSatCount;Gear;Brake_pedal;Accel_pedal;Machine_Speed_Mesured;AST_Direction;Ast_HPMB1_Pressure_bar;Ast_HPMA_Pressure_bar;Pressure_HighPressureReturn;Pressure_HighPressure;Oil_Temperature;Ast_FrontAxleSpeed_Rpm;Pump_Speed"

row_count = 0
file_count = 0
max_rows_per_file = 10
data_buffer = []
start_time = time.time()

def rand():
    current = datetime.datetime.now()
    return (str(current) + ';' + str(random.uniform(0, 87.06)) + ';' + str(random.randint(0, 255)) + ';' + str(random.randint(124, 137)) + ';' + str(random.randint(124, 137)) + ';' + str(random.randint(0, 100)) + ';' + '20;20;' + str(random.randint(0, 13)) + ';' + str(random.randint(0, 13)) + ';' + str(random.randint(0, 13)) + ';' + str(random.randint(32767, 33195)) + ';' + str(random.randint(11, 255)) + ';' + str(random.randint(32767, 33195)) + ';' + str(random.randint(577, 1799)))

def save_data():
    global file_count, data_buffer
    if data_buffer:
        file_count += 1
        current = datetime.datetime.now()
        file_name = f"X467898_{str(current).replace(':', '_')}.csv"
        rows = [row.split(';') for row in data_buffer]
        df = pd.DataFrame(rows, columns=header.split(';'))
        df.to_csv(file_name, index=False)
        print(f"Saved {len(data_buffer)} rows to {file_name}")
        data_buffer = []
    threading.Timer(60, save_data).start()



def printit():
    global row_count, data_buffer, start_time
    current = datetime.datetime.now()
    row_count += 1
    data = rand()
    data_buffer.append(data)
    print(data)
    if row_count % max_rows_per_file == 0:
        save_data()
    if time.time() - start_time > 60:
        print("Stopping threads...")
        threading.Timer(1, print, args=("Stopping threads...",)).start()
        for t in threading.enumerate():
            if t != threading.current_thread():
                t.cancel()
    else:
        threading.Timer(0.1, printit).start()

save_data()
printit()


# In[ ]:



