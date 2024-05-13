import pandas as pd
import polars as pl

def GetMidPrice(orderbook):
    orderbook_bid = orderbook.filter(pl.col('type') == 0)
    orderbook_ask = orderbook.filter(pl.col('type') == 1)
    
    orderbook_bid_top = orderbook_bid.group_by('timestamp').first().sort('timestamp')
    orderbook_ask_top = orderbook_ask.group_by('timestamp').first().sort('timestamp')
    
    orderbook_bid_mean = orderbook_bid.group_by('timestamp').agg(pl.col('price').mean()).sort('timestamp')
    orderbook_ask_mean = orderbook_ask.group_by('timestamp').agg(pl.col('price').mean()).sort('timestamp')

    mid_price = orderbook_ask_top.select(pl.col('timestamp')).sort('timestamp')
    
    mid = (orderbook_ask_top.select(pl.col('price')) + orderbook_bid_top.select(pl.col('price'))) * 0.5
    mid_price = mid_price.with_columns(mid.select(pl.col('price'))).rename({'price' : 'midprice'})

    mid_mean = (orderbook_ask_mean.select(pl.col('price')) + orderbook_bid_mean.select(pl.col('price'))) * 0.5
    mid_price = mid_price.with_columns(mid_mean.select(pl.col('price'))).rename({'price' : 'midprice_mean'})
    
    mid_mkt = (orderbook_bid_top.select(pl.col('price')) * orderbook_ask_top.select(pl.col('quantity')) + \
            orderbook_ask_top.select(pl.col('price')) * orderbook_bid_top.select(pl.col('quantity'))) / \
            (orderbook_bid_top.select(pl.col('quantity')) + orderbook_ask_top.select(pl.col('quantity')))
    mid_price = mid_price.with_columns(mid_mkt.select(pl.col('price'))).rename({'price' : 'midprice_mkt'})
    mid_price = mid_price.with_columns(mid_price.select(pl.col('midprice_mkt').round(3).alias("midprice_mkt_round"))).drop("midprice_mkt")
    mid_price = mid_price.rename({'midprice_mkt_round' : 'midprice_mkt'})
    
    return mid_price

def BookImbalance(orderbook, ratio, interval, midprice):
    orderbook_bid = orderbook.filter(pl.col('type') == 0)
    orderbook_ask = orderbook.filter(pl.col('type') == 1)
    
    orderbook_bid = orderbook_bid.with_columns(orderbook_bid.select((pl.col('quantity') ** ratio).alias("qty")))
    orderbook_ask = orderbook_ask.with_columns(orderbook_ask.select((pl.col('quantity') ** ratio).alias("qty")))
    
    orderbook_bid = orderbook_bid.with_columns(orderbook_bid.select((pl.col('price')*pl.col("qty")).alias("px")))
    orderbook_ask = orderbook_ask.with_columns(orderbook_ask.select((pl.col('price')*pl.col("qty")).alias("px")))
    
    bid_qty_px = orderbook_bid.group_by('timestamp').agg(pl.col('qty').sum(), pl.col('px').sum()).sort('timestamp')
    ask_qty_px = orderbook_ask.group_by('timestamp').agg(pl.col('qty').sum(), pl.col('px').sum()).sort('timestamp')
    
    book_price = (ask_qty_px.select(pl.col('qty')) * bid_qty_px.select(pl.col('px')) / bid_qty_px.select(pl.col('qty')) + \
            bid_qty_px.select(pl.col('qty')) * ask_qty_px.select(pl.col('px')) / ask_qty_px.select(pl.col('qty'))) / \
            (ask_qty_px.select(pl.col('qty')) + bid_qty_px.select(pl.col('qty')))
    book_price = book_price.rename({'qty' : 'bookprice'})
    
    book_imbalance = bid_qty_px.select(pl.col('timestamp')).sort('timestamp')
    
    imbalance = (book_price.select(pl.col('bookprice')) - midprice.select(pl.col('midprice'))) / interval
    book_imbalance = book_imbalance.with_columns(imbalance.select(pl.col('bookprice').round(3)).rename({'bookprice' : 'book-imbalance-'+str(ratio)+'-5-'+str(interval)}))

    return book_imbalance

PATH = "./" 
  
def main():
    pl.Config(set_fmt_float = "full")
    global PATH
    orderbook = pl.read_csv(PATH + "book-2024-04-09-exchange-market.csv")
    mid_price = GetMidPrice(orderbook)
    book_imbalance = BookImbalance(orderbook, 0.2, 1, mid_price)
    features = pl.concat([mid_price, book_imbalance], how = 'align')
    features.write_csv(PATH + "2024-04-09-exchange-market-feature.csv")
    
    orderbook = pl.read_csv(PATH + "book-2024-04-10-exchange-market.csv")
    mid_price = GetMidPrice(orderbook)
    book_imbalance = BookImbalance(orderbook, 0.2, 1, mid_price)
    features = pl.concat([mid_price, book_imbalance], how = 'align')
    features.write_csv(PATH + "2024-04-10-exchange-market-feature.csv")

if __name__ == '__main__':
    main()