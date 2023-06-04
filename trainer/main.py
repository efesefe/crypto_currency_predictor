import os
from dotenv import load_dotenv
from pymongo import MongoClient
import numpy as np
from pyspark.sql import SparkSession
import pyspark
spark = SparkSession.builder.master('local').getOrCreate()

load_dotenv(dotenv_path=".env")

uri = "mongodb+srv://cryptocluster.6p8ex3z.mongodb.net/?authSource=%24external&authMechanism=MONGODB-X509&retryWrites=true&w=majority"

client = MongoClient(uri,
                     tls=True,
                     tlsCertificateKeyFile='./X509-efe.pem')

db = client['CryptoData']

headers = {
  'Content-Type': 'application/json',
  'Access-Control-Request-Headers': '*',
  'api-key': os.getenv('API_KEY'), 
}

trees = {}

TIME_STEPS = 5

def create_sequences(X, y, time_steps = TIME_STEPS):
    Xs, ys = [], []

    for i in range(len(X) - time_steps):
      Xs.append(X[i : (i + time_steps)])
      ys.append(y[i + time_steps])

    return np.array(Xs), np.array(ys)

def main():
  COIN_SYMBOLS = db.list_collection_names()
  for symbol in COIN_SYMBOLS:
    data = db[symbol].find(filter={},projection={"USD":1})
    data = list(data)
    data = [dat['USD'] for dat in data]
    print(data)
    X_train, y_train = create_sequences(data, data)
    print("---X------X------X------X------X---")
    print(X_train)
    sdf = spark.createDataFrame(X_train)
    sdf.printSchema() #data type of each column
    sdf.show(5) #It gives you head of pandas DataFrame
    count_df = sdf.count()
    print(count_df)
    print("---Y------Y------Y------Y------Y---")
    print(y_train.shape)

main()