import json
from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS
from log_configure import logger
from config import *
from confluent_kafka import Consumer

consumer_config = {
    'bootstrap.servers': KAFKA_BROKER,
    'group.id': 'consumer_group_1',
    'auto.offset.reset': 'earliest',

}
consumer = Consumer(consumer_config)
consumer.subscribe([KAFKA_TOPIC_THERMOSTAT])
client = InfluxDBClient(url=url, token=token)

# Function to store payload in Influxdb database
def store_in_influx(line_protocol):
    print("Storing started")
    try:
        ######temperature#################
        thermostat_device_id = line_protocol['device']
        get_temp_reading = line_protocol['temperature']
        get_thermostat_timestamp = line_protocol['timestamp']
        # Write data to InfluxDB
        data = [
            {
                "measurement": "measurement_thermostat",
                "tags": {"device": "" + thermostat_device_id+ ""},
                "time": "" + get_thermostat_timestamp + "",
                "fields": {"temperature": get_temp_reading}
            }
        ]
        write_api = client.write_api(write_options=SYNCHRONOUS)
        write_api.write(bucket=bucket, org=org, record=data)
        logger.info("Storing data" + str(line_protocol) + "to the database")
    except Exception as e:
        logger.error("An error occurred while storing data in InfluxDB: " + str(e))
        print(e)
        return f"Error: {e}"

def consume_kafka_msg():
    while True:
        msg = consumer.poll(5.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:  # If there are no new messages in Kafka
                continue
            else:
                break
        payload = msg.value().decode('utf-8').strip()
        try:
            payload_dict = json.loads(payload.replace("'", "\""))
            store_in_influx(payload_dict)
        except json.JSONDecodeError as e:
            logger.error("JSON decoding error: " + str(e))

consume_kafka_msg()