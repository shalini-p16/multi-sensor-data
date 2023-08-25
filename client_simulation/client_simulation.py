import random
import time
import requests
from confluent_kafka import Producer
import json
from log_configure import logger
from flask import Flask, request, jsonify

app = Flask(__name__)

api_url = "http://127.0.0.1:5000/add-readings"

class IoTDevice:
    def __init__(self, name):
        self.name = name

    def generate_data(self):
        raise NotImplementedError("Subclasses must implement generate_data method")

class CarFuelReading(IoTDevice):
    def generate_data(self):
        fuel_reading = round(random.uniform(10, 60), 2)
        timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
        data = {
            "device": self.name,
            "fuel_reading": str(fuel_reading),
            "timestamp": "" + timestamp + ""
        }
        return {"device": ""+self.name+"", "fuel_reading": ""+str(fuel_reading)+"", "timestamp": "" + timestamp + ""} #Json formatted string


class HeartRateSensor(IoTDevice):
    def generate_data(self):
        heart_rate = random.randint(60, 120)
        timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
        data = {
            "device": ""+ self.name +"",
            "heart_rate": ""+str(heart_rate)+"",
            "timestamp": "" + timestamp + ""
        }
        return {"device": ""+self.name+"", "heart_rate": ""+str(heart_rate)+"", "timestamp": "" + timestamp + ""} #Json formatted string

class Thermostat(IoTDevice):
    def generate_data(self):
        temperature = round(random.uniform(18, 30), 2)
        timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
        data = {
            "device": self.name,
            "temperature": str(temperature),
            "timestamp": "" + timestamp + ""
        }
        return {"device": ""+self.name+"", "temperature": ""+str(temperature)+"", "timestamp": "" + timestamp + ""} #Json formatted string

devices = [
    CarFuelReading("Car1"),
    HeartRateSensor("Patient1"),
    Thermostat("Home1")
]


while True:
    for device in devices:
        data = device.generate_data()
        logger.info("Data generated is" +str(data))
        try:
            response = requests.post(api_url, json=data)
            print("-" * 30)
            time.sleep(1)
        except json.JSONDecodeError as e:
            print("Error decoding JSON:", e)


if __name__ == '__main__':
    app.run(debug=True)






