import json
import paho.mqtt.client as mqtt

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
client.publish("vehicle/sensor_data", json_message)

# Disconnect from the broker
client.disconnect()
