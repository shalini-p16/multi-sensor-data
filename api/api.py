from flask import Flask, request, jsonify
from influxdb_client import InfluxDBClient, Point
from log_configure import logger
from influxdb_client.client.write_api import SYNCHRONOUS
from datetime import datetime, timedelta
from api import *
from config import KAFKA_TOPIC_CAR,KAFKA_TOPIC_HEARTRATE,KAFKA_TOPIC_THERMOSTAT,bucket,url,token,org,start_time,end_time
from confluent_kafka import Producer
import json
import os

ROOT_DIR = os.path.dirname(os.path.abspath(__file__)) #Project Root
print(ROOT_DIR)

app = Flask(__name__)

client = InfluxDBClient(url=url, token=token, org=org)
KAFKA_BROKER = 'localhost:9092'
producer_config = {
    'bootstrap.servers': KAFKA_BROKER,
     #'value.serializer': lambda v: json.dumps(v.encode('utf-8'))
}
producer = Producer(producer_config)

#Function to extract payload from POST request
@app.route('/add-readings', methods=['POST'])
def add_reading():
    logger.info("Extracting data from a POST request")
    data = request.json
    #filter_data = json.loads(data)# Remove the parentheses here
    filter_data = data
    print(filter_data)
    print(type(filter_data))

    # Check if the 'device' key is present in the JSON data
    if 'device' in filter_data:
        device_type = filter_data["device"]

        if device_type == "Car1":
            # Process Car Fuel Reading data
            fuel_reading = filter_data['fuel_reading']
            publish_to_kafka(filter_data, KAFKA_TOPIC_CAR)

        elif device_type == "Patient1":
            heart_rate = filter_data['heart_rate']
            publish_to_kafka(filter_data, KAFKA_TOPIC_HEARTRATE)

        elif device_type == "Home1":
            temperature= filter_data['temperature']
            print(temperature)
            publish_to_kafka(filter_data, KAFKA_TOPIC_THERMOSTAT)

        else:
            return jsonify({'message': 'Invalid device type'}), 400

        return jsonify({'status': 'Data added successfully'})

    else:
        return jsonify({'message': 'Missing device type'}), 400


#Function to get statistics
@app.route('/get-statistics', methods=['GET'])
def get_statistics():
    start_time = request.args.get('start_time')
    end_time = request.args.get('end_time')
    statistics = calculate_statistics(bucket, start_time, end_time)
    return jsonify(statistics)
    # except Exception as e:
    #     error_message = f"An error occurred: {str(e)}"
    #     return jsonify(error=error_message), 500  # Return a 500 Internal Server Error status code

#Function to serialize datetime object to JSON
class CustomEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)

#Calculate statistics based on the timeframe
def calculate_statistics(bucket, start_time, end_time):
    print("calculating statistics")
    query = f'''
        from(bucket: "multi_sensor_data")
        |> range(start: 2023-08-25T00:00:00Z, stop: 2023-08-25T07:14:24Z) 
        |> filter(fn: (r) => r._measurement == "measurement_thermostat")
        |> filter(fn: (r) => r.device == "Home1" )
        |> keep(columns: ["_time", "_value", "temperature"])
        |> duplicate(column: "_value", as: "temperature")
        |> map(fn: (r) => ({{r with temperature: float(v: r.temperature)}}))
        |> group()
        |> mean(column: "temperature")
    '''

    query_api = client.query_api()
    result = query_api.query(org=org, query=query)
    data = []
    for table in result:
        for record in table.records:
            data.append(record.values)
    get_final_data = json.dumps(data,cls=CustomEncoder)
    print("final json result is" +get_final_data)
    logger.info("Got json from database based on the conditions" +get_final_data)
    return get_final_data


#Function to publish payload to Kafka topic
def publish_to_kafka(payload, KAFKA_TOPIC):
    try:
        logger.info("Publishing the data to Kafka topic")
        producer.produce(KAFKA_TOPIC, value=str(payload))
        producer.flush()
        logger.info("Data published")
        return jsonify({'message': 'Success'})
    except Exception as e:
        error_message = f"An error occurred while publishing to Kafka: {str(e)}"
        return jsonify(error=error_message), 500  # Return a 500 Internal Server Error status code

if __name__ == '__main__':
    app.run(debug=True)



