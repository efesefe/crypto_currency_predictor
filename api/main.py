import json
from kafka import KafkaProducer
from json import dumps
import requests
from datetime import datetime
from time import sleep

COIN_SYMBOLS = ["BTC","ETH","USDT","BNB","USDC","XRP","ADA","DOGE","SOL","MATIC","LTC",
                "TRX","DOT","BUSD","SHIB","AVAX","DAI","WBTC","LINK","LEO","UNI","ATOM",
                "XMR","OKB","ETC","XLM","TON","ICP","BCH","TUSD","FIL","LDO","APT","HBAR",
                "CRO","NEAR","ARB","VET","APE","QNT","ALGO","GRT","USDP","FTM","SAND","EOS",
                "RPL","EGLD","AAVE","MANA","BIT","THETA","STX","RNDR","CFX","XTZ","AXS","FLOW",
                "CHZ","USDD","KCS","IMX","CRV","NEO","BSV","SNX","MKR","PEPE","SUI","GUSD","OP",
                "BTT","INJ","ZEC","KLAY","GMX","PAXG","LUNC","CSPR","MINA","FXS","KAVA","MIOTA",
                "XEC","DASH","GT","HT","TWT","FLR","XDC","LRC","WOO","NEXO","ZIL","RUNE","CAKE",
                "MASK","CVX","ENJ","DYDX"]

COINS_PEPE = "BTC,ETH,USDT,BNB,USDC,XRP,ADA,DOGE,SOL,MATIC,LTC,TRX,DOT,BUSD,SHIB,AVAX, \
        DAI,WBTC,LINK,LEO,UNI,ATOM,XMR,OKB,ETC,XLM,TON,ICP,BCH,TUSD,FIL,LDO,APT,HBAR, \
        CRO, NEAR,ARB,VET,APE,QNT,ALGO,GRT,USDP,FTM,SAND,EOS,RPL,EGLD,AAVE,MANA,BIT, \
        THETA, STX,RNDR,CFX,XTZ,AXS,FLOW,CHZ"

COINS_DYDX = "BSV,SNX,MKR,PEPE,SUI,GUSD,OP,BTT,INJ,ZEC,KLAY,GMX,PAXG,LUNC,CSPR, \
    MINA,FXS, KAVA,MIOTA,XEC, DASH,GT,HT,TWT,FLR,XDC,LRC,WOO,NEXO,ZIL,RUNE,CAKE,MASK, \
    CVX,ENJ,DYDX,NEO,CRV,USDD,KCS,IMX"

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: 
                         dumps(x).encode('utf-8'))

def all_coins_USD_TL():
    while True:
        now = datetime.now()
        url = f"https://min-api.cryptocompare.com/data/pricemulti?fsyms={COINS_PEPE}&tsyms=USD,EUR,TRY"
        resp = requests.get(url)
        dic = resp.json()
        url = f"https://min-api.cryptocompare.com/data/pricemulti?fsyms={COINS_DYDX}&tsyms=USD,EUR,TRY"
        resp = requests.get(url)
        dic.update(resp.json())
        dic.update({"time":now.isoformat()})
        producer.send('coin_test', value=dic)
        print(json.dumps(dic, indent=4))
        sleep(60)

all_coins_USD_TL()
