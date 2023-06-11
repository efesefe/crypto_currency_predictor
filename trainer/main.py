from dotenv import load_dotenv
from pymongo import MongoClient
import numpy as np
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession

from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Dense
from tensorflow.keras.layers import LSTM

spark = SparkSession.builder.master('local').getOrCreate()

load_dotenv(dotenv_path=".env")

uri = "mongodb+srv://cryptocluster.6p8ex3z.mongodb.net/?authSource=%24external&authMechanism=MONGODB-X509&retryWrites=true&w=majority"

client = MongoClient(uri,
                     tls=True,
                     tlsCertificateKeyFile='./X509-efe.pem')

db = client['CryptoData']

TIME_STEPS = 2

def create_sequences(X, y, time_steps = TIME_STEPS):
    Xs, ys = [], []

    for i in range(len(X) - time_steps):
      Xs.append(X[i : (i + time_steps)])
      ys.append(y[i + time_steps])

    return np.array(Xs), np.array(ys)

def main():
  COIN_SYMBOLS = db.list_collection_names()
  for symbol in COIN_SYMBOLS:
    data = db[symbol].find(filter={},projection={"USD":1}, limit=2000)
    data = list(data)
    data = [dat['USD'] for dat in data]
    data = np.array(data)
    print(data)
    training_size = int(len(data) * 0.60)
    test_size=len(data)-training_size
    train_data , test_data = data[0 : training_size] , data[test_size : len(data)]
    X_train, y_train = create_sequences(train_data, train_data)
    X_test, y_test = create_sequences(test_data, test_data)
    sdf = spark.createDataFrame(X_train)
    sdf.printSchema()
    sdf.show(5)
    count_df = sdf.count()
    model = Sequential()
    model.add(LSTM(10, input_shape=(None,1),activation="relu"))
    model.add(Dense(1))
    model.compile(loss="mean_squared_error",optimizer="adam")
    history = model.fit(X_train, y_train, validation_data = (X_test, y_test),
                        epochs=200, batch_size=32, verbose=1)
    model.save(f"models/{symbol}_model.h5")
    

    loss = history.history['loss']
    val_loss = history.history['val_loss']

    epochs = range(len(loss))

    plt.plot(epochs, loss, 'r', label='Training loss')
    plt.plot(epochs, val_loss, 'b', label='Validation loss')
    plt.title('Training and validation loss')
    plt.legend(loc=0)

    plt.show()

    print(count_df)

main()