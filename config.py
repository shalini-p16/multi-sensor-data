import json
from confluent_kafka import Producer

#########Kafka Configuration###################
KAFKA_BROKER = 'localhost:9092'
KAFKA_TOPIC_CAR = 'car_data_statistics_a'
KAFKA_TOPIC_THERMOSTAT = 'thermostat_data_statistics_a'
KAFKA_TOPIC_HEARTRATE = 'heartrate_data_statistics_a'

producer_config = {
    'bootstrap.servers': KAFKA_BROKER,
     #'value.serializer': lambda v: json.dumps(v.encode('utf-8'))
}
producer = Producer(producer_config)

##################### InfluxDB configuration###############
url = "http://localhost:8086"
org = "myorg"
token = "W59Nu6pfAs1pmpHLbjdWqqSRgmFoG4z55I_rkUsRLU08e2zMzaBsGFwP24AVIiijWqt4QGckSM0lmFBJAG7crA=="
bucket = "multi_sensor_data"

######## start time, and end time for query###################
start_time = "2023-08-25T00:00:00Z"
end_time = "2023-08-25T07:14:24Z"
device="Home1"

# API to publish car readings
api_url = "http://127.0.0.1:5000/get-readings"

