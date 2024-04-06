import time
from datetime import datetime
import requests
import pandas as pd
import argparse

def write_csv(df):
    df.to_csv("Orderbook.csv", header = False, index = False, mode = 'a')

def get_orderbook():
    #Generate new file
    col = ["price", "quantity", "type", "time", "currency"]
    df = pd.DataFrame(columns = col)
    df.to_csv("Orderbook.csv", index = False, mode = 'w')

    time_last = datetime.now()
    
    #Get orderbook data and write on orderbook.csv file
    while(1):
        #Get orderbook in 1 sec interval
        time_now = datetime.now()
        if (time_now - time_last).total_seconds() < 1.0:
            continue
        time_last = time_now
        
        response = requests.get(DataPath)
        response_data = response.json()["data"]
        response_time = time_now.strftime('%Y-%m-%d %H:%M:%S.%f')
        print("Response time is " + response_time + ", response status is " + str(response.status_code))
        df_bid = pd.DataFrame(response_data["bids"]).sort_values(by = "price", ascending = False)
        df_bid['type'] = "bids"
        df_ask = pd.DataFrame(response_data["asks"]).sort_values(by = "price", ascending = True)
        df_ask['type'] = "asks"
        df = pd.concat([df_bid, df_ask])
        df['time'] = response_time
        df['currency'] = currency
        df = df.reset_index().drop('index', axis = 1)
        write_csv(df)

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--currency", help = "what crypto currency to get", choices = ['BTC'], dest = "currency", action = "store")
    parser.add_argument("--count", help = "how many orderbook limes to get", choices = ['5', '10'], dest = "count", action = "store")
    
    return parser.parse_args()

curency = ''
count = ''
DataPath = ''

def main():
    
    global currency
    global count
    global DataPath
    
    args = parse_args()
    
    currency = args.currency
    count = args.count
    DataPath = 'https://api.bithumb.com/public/orderbook/' + currency + '_KRW/?count=' + count
    
    get_orderbook()
    
if __name__ == '__main__':
    main()