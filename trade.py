import time
from datetime import datetime
import requests
import pandas as pd
import argparse
from requests.sessions import Session
from requests.adapters import Retry, HTTPAdapter
from urllib3.exceptions import InsecureRequestWarning
from urllib3 import disable_warnings
import warnings

def write_csv(df):
    global timestamp
    new_timestamp = df.loc[0, "timestamp"].split(' ')[0]
    if new_timestamp != timestamp:
        df.to_csv("trade-"+new_timestamp+"-exchange-market.csv", header = True, index = False, mode = 'w')
        timestamp = new_timestamp
    else:
        df.to_csv("trade-"+timestamp+"-exchange-market.csv", header = False, index = False, mode = 'a')

def find_start(df1, df2):
    df1 = df1.drop_duplicates()
    last_point = df2.tail(1)
    df_check = pd.concat([df1, last_point])
    start_point = df_check.index[(df_check.duplicated(keep = 'last') == True)].tolist()
    if not start_point:
        return 0
    return start_point[0]

def get_response(url):
    try:
        response = session.get(DataPath_trade, verify = False, timeout = 1, allow_redirects = True)
        response_data = response.json()["data"]
        response_status = response.status_code
    
        return response_data, response_status
    except:
        return None, "Response Error"

data_trade_last = pd.DataFrame(columns = ['tranaction_date', 'type', 'units_traded', 'price', 'total'])
def get_trade(res_time):
    global data_trade_last
    data_trade, status_trade = get_response(DataPath_trade)
    data_trade = pd.DataFrame(data_trade)
    if data_trade is None:
        return status_trade
    if data_trade_last.empty:
        data_trade_last = data_trade
        return status_trade
    
    start_point = find_start(data_trade, data_trade_last)
    data_trade_last = data_trade
    data_trade_new = data_trade[(start_point+1):]
    if data_trade_new.empty:
        return "No trade data"
    
    df_bid = data_trade_new.loc[data_trade_new["type"]=='bid',:].apply(pd.to_numeric, errors = 'ignore')
    df_ask = data_trade_new.loc[data_trade_new["type"]=='ask',:].apply(pd.to_numeric, errors = 'ignore')
    if not df_bid.empty:
        bid_quant = df_bid['units_traded'].sum()
        bid_total = df_bid['total'].sum()
        bid_price = bid_total/bid_quant
        trade_bid = pd.DataFrame([[bid_quant, bid_price, bid_total, 0, len(df_bid)]], columns = ["units_traded", "price", "total", "type", "count"])
    else:
        trade_bid = pd.DataFrame(columns = ["units_traded", "price", "total", "type", "count"])
    if not df_ask.empty:
        ask_quant = df_ask['units_traded'].sum()
        ask_total = df_ask['total'].sum()
        ask_price = ask_total/ask_quant
        trade_ask = pd.DataFrame([[ask_quant, ask_price, ask_total, 1, len(df_ask)]], columns = ["units_traded", "price", "total", "type", "count"]) 
    else:
        trade_ask = pd.DataFrame(columns = ["units_traded", "price", "total", "type", "count"])
    df_trade = pd.concat([trade_bid, trade_ask])
    if not df_trade.empty:
        df_trade["timestamp"] = res_time
        df_trade = df_trade.reset_index().drop('index', axis = 1)
        write_csv(df_trade)
    return status_trade

def get_write_trade():
    time_start = datetime.now()
    time_last = time_start
    time_now = time_start
    
    #Collect orderbook data while 1 day(=86400sec)
    while (time_now - time_start).total_seconds() <= 86400:
        #Get orderbook in 1 sec interval
        time_now = datetime.now()
        if (time_now - time_last).total_seconds() < 1.0:
            continue
        time_last = time_now
        
        response_time = time_now.strftime('%Y-%m-%d %H:%M:%S.%f')
        status = get_trade(response_time)
        if status == "Response Error":
            print("Trade : ", ((time_now - time_start).total_seconds()/86400)*100, "% is done.")
            print(status + "Response time is " + response_time)
            continue
        elif status == "No trade data":
            print("Trade : ", ((time_now - time_start).total_seconds()/86400)*100, "% is done.")
            print(status + "Response time is " + response_time)
        else:
            print("Trade : ", ((time_now - time_start).total_seconds()/86400)*100, "% is done.")
            print("Response status is " + str(status) + ", Response time is " + response_time)

#Decide what currency and how many lines to get orderbook
def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--currency", help = "what crypto currency to get", choices = ['BTC', 'ETH'], dest = "currency", action = "store")
    return parser.parse_args()

#Make request session
def init_session():
    session = requests.Session()
    #Update header
    my_header = {'User-Agent' : 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36'}
    session.headers.update(my_header)
    
    connect = 1
    backoff_factor = 0.1
    retry_status = ()
    
    #Set retry when connection fail
    retry = Retry(total = (connect+1), backoff_factor = backoff_factor, status_forcelist = retry_status)
    adaptor = HTTPAdapter(max_retries = retry)
    
    #Use retry while url starts with 'http://' or 'https://'
    session.mount("http://", adaptor)
    session.mount("https://", adaptor)
    
    return session

timestamp = ''
curency = ''
count = ''
DataPath_trade = ''
session = init_session()

def main():
    disable_warnings(InsecureRequestWarning)
    warnings.simplefilter(action = 'ignore', category = FutureWarning)
    
    global currency
    global count
    global DataPath_trade
    
    args = parse_args()
    currency = args.currency
    DataPath_trade = 'https://api.bithumb.com/public/transaction_history/' + currency + '_KRW/?count=50'
    
    get_write_trade()
    
    session.close()
    
if __name__ == '__main__':
    main()