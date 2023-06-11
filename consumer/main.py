from kafka import KafkaConsumer
from json import loads
from pymongo import MongoClient

from dotenv import load_dotenv

load_dotenv(dotenv_path=".env")

uri = "mongodb+srv://cryptocluster.6p8ex3z.mongodb.net/?authSource=%24external&authMechanism=MONGODB-X509&retryWrites=true&w=majority"

client = MongoClient(uri,
                     tls=True,
                     tlsCertificateKeyFile='./X509-efe.pem')

db = client['CryptoData']

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
                db[symbol].insert_one(data)
        print(f'{time} added')

main()