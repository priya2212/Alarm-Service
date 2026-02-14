To run this, install dependecines - 
1. Python 3.6 or more
2. paho-mqtt library
3. sqlite3

Once, the dependencies are installed, download the repo and go to the main folder and execute following command - 
python3 main.py

**Publisher Client - 
**

Threshold condition type - 
 watch -n 1 "mosquitto_pub -t telemetry/sensor1/temperature -m '{\"value\":25}'"
 
Conditional condition type
mosquitto_pub -t telemetry/sensor2/current -m '{"value":10}'
watch -n 1 "mosquitto_pub -t telemetry/sensor1/temperature -m '{\"value\":25}'"


**Subscriber client - 
**

Topic - mosquitto_sub -t alarm/# -v

Threshold Alarm trigger resp - 

alarm/sensor1/temperature {"rule_id": 1, "device_id": "sensor1", "sensor_id": "temperature", "value": 25, "triggered_at": "2026-02-14T15:03:03.184944+00:00", "message": "ALARM TRIGGERED"}

Conditional Alarm trigger resp - 

alarm/sensor1/temperature {"rule_id": 2, "device_id": "sensor1", "sensor_id": "temperature", "value": 25, "triggered_at": "2026-02-14T15:02:58.142694+00:00", "message": "ALARM TRIGGERED"}
