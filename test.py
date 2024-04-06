import time
import requests
import pandas as pd

currency = 'BTC'
count = 10
DataPath = 'https://api.bithumb.com/public/orderbook/' + currency + '_KRW/?count=' + str(count)

def write_csv(df):
    df.to_csv(mode = 'a')

def get_orderbook():
    response = requests.get(DataPath)
    response_data = response.json()["data"]
    df_bid = pd.DataFrame(response_data["bids"]).sort_values(by = "price", ascending = False)
    df_bid['type'] = "bids"
    df_ask = pd.DataFrame(response_data["asks"]).sort_values(by = "price", ascending = True)
    df_ask['type'] = "asks"
    df = pd.concat([df_bid, df_ask])
    df['time'] = response_data["timestamp"]
    df['currency'] = currency
    df = df.reset_index().drop('index', axis = 1)
    write_csv()
    print(df)
    
    
def main():
    get_orderbook()
    
if __name__ == '__main__':
    main()