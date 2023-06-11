import json
from kafka import KafkaProducer
from json import dumps
import requests
from datetime import datetime
from time import sleep

COINS = "BTC,ETH,USDT,BNB,USDC,XRP,ADA,DOGE,SOL,MATIC,LTC,TRX,DOT,BUSD,SHIB,AVAX, \
        DAI,WBTC,LINK,LEO,UNI,ATOM,XMR,OKB,ETC"

producer = KafkaProducer(bootstrap_servers = ['localhost:9092'],
                         value_serializer=lambda x: 
                         dumps(x).encode('utf-8'))

def all_coins_USD_TL():
    while True:
        now = datetime.now()
        url = f"https://min-api.cryptocompare.com/data/pricemulti?fsyms={COINS}&tsyms=USD,EUR,TRY"
        resp = requests.get(url)
        dic = resp.json()
        dic.update({"time" : now.isoformat()})
        producer.send('coin_test', value=dic)
        print(json.dumps(dic, indent = 4))
        sleep(60)

all_coins_USD_TL()
