import json
import paho.mqtt.client as mqtt
import sys
import dotenv as dtenv
dtenv.load_dotenv()
import os
# Define the callback function that will handle incoming messages
def on_message(client, userdata, message):
    # Decode the JSON message
    json_message = message.payload.decode('utf-8')
    message_data = json.loads(json_message)

    # Print the message data
    print('----------------------')
    print("Received message:")
    print("Date and Hour: ", message_data["dateHour"])
    print("GPS Speed: ", message_data["gpsSpeed"])
    print("GPS Sat Count: ", message_data["gpsSatCount"])
    print("Gear: ", message_data["Gear"])
    print("Brake Pedal: ", message_data["Brake_pedal"])
    print("Accel Pedal: ", message_data["Accel_pedal"])
    print("Machine Speed Measured: ", message_data["Machine_Speed_Mesured"])
    print("AST Direction: ", message_data["AST_Direction"])
    print("AST HPMB1 Pressure (bar): ", message_data["Ast_HPMB1_Pressure_bar"])
    print("AST HPMA Pressure (bar): ", message_data["Ast_HPMA_Pressure_bar"])
    print("Pressure High Pressure Return: ", message_data["Pressure_HighPressureReturn"])
    print("Pressure High Pressure: ", message_data["Pressure_HighPressure"])
    print("Oil Temperature: ", message_data["Oil_Temperature"])
    print("AST Front Axle Speed (RPM): ", message_data["Ast_FrontAxleSpeed_Rpm"])
    print("Pump Speed: ", message_data["Pump_Speed"])
    print('----------------------')

# Connect to the MQTT broker
client = mqtt.Client()
client.on_message = on_message

if client.connect("0.0.0.0", 1883, 60) != 0:
    print("could not connect to mqtt broker!")
    sys.exit(-1)

# Subscribe to the "vehicle/sensor_data" topic
client.subscribe( topic = os.environ.get("EMQX_TOPIC_NAME"))

# Start the MQTT client loop
try:
    print("press ctrl+c to quit....")
    client.loop_forever()
except:
    print("disconnecting broker")

client.disconnect()