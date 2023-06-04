from kafka import KafkaConsumer
from json import loads

import requests
import json
import os
from dotenv import load_dotenv

load_dotenv(dotenv_path=".env")

url = "https://eu-central-1.aws.data.mongodb-api.com/app/data-quqkg/endpoint/data/v1/action/insertOne"


headers = {
  'Content-Type': 'application/json',
  'Access-Control-Request-Headers': '*',
  'api-key': os.getenv('API_KEY'), 
}

consumer = KafkaConsumer(
    'coin_test',
     bootstrap_servers=['localhost:9092'],
     auto_offset_reset='earliest',
     enable_auto_commit=True,
     group_id='my-group',
     value_deserializer=lambda x: loads(x.decode('utf-8')))

def main():
    for message in consumer:
        message = message.value
        time = message['time']
        for symbol in message:
            if symbol != "time":
                data = message[symbol]
                data.update({'time':time})
                data.update({'learn':True})
                payload = json.dumps({
                    "collection": symbol,
                    "database": "CryptoData",
                    "dataSource": "CryptoCluster",
                    "document": data
                })
                requests.request("POST", url, headers=headers, data=payload)
        print(f'{time} added')

main()