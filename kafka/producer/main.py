import json
from datetime import datetime

import requests
import xmltodict
import confluent_kafka
from confluent_kafka import Producer
import socket


class Requests_handler():
    url = 'https://api.worldweatheronline.com/premium/v1/weather.ashx?key=45b084f9b45d4deca56125148242608&q=61.793673, 34.338652&cc=yes&fx=no'
    response = None

    def __init__(self):
        self.response = requests.get(self.url)

    def getting_data(self):
        self.response = requests.get(self.url)


    def debager(self):
        print( self.response.text)

class kafka_producer():
    producer = None

    def __init__(self):
        conf = {
            'bootstrap.servers': 'localhost:9092',
            'client.id': socket.gethostname()
        }
        self.producer = confluent_kafka.Producer(conf)

    def send_to_kafka(self, value):
        self.producer.produce(topic='weather', key=f'date:{datetime.now()}', value=value)
        self.producer.flush()


rh = Requests_handler()

rh.getting_data()
rh.debager()
kafka = kafka_producer()

kafka.send_to_kafka(rh.response.text)