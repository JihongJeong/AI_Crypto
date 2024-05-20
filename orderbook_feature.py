import pandas as pd
import polars as pl
import numpy as np

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

def BookImbalance(orderbook, ratio, interval, midprice, mid_type):
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
    
    imbalance = (book_price.select(pl.col('bookprice')) - midprice.select(pl.col(mid_type))) / interval
    book_imbalance = book_imbalance.with_columns(imbalance.select(pl.col('bookprice').round(3)).rename({'bookprice' : 'book-imbalance-{:.1f}-5-{}-{}'.format(ratio, interval, mid_type)}))

    return book_imbalance

def BookDelta(orderbook, ratio):
    orderbook_bid = orderbook.filter(pl.col('type') == 0)
    orderbook_ask = orderbook.filter(pl.col('type') == 1)
    
    bid_qty_top = orderbook_bid.group_by('timestamp').agg(pl.col('quantity').sum(), pl.col('price').max()).sort('timestamp')
    bid_qty_top = bid_qty_top.rename({'quantity' : 'curBidQty', 'price' : 'curBidTop'})
    prev_bid_qty = bid_qty_top.select(pl.col('curBidQty')).to_numpy().T
    prev_bid_qty = np.pad(prev_bid_qty.ravel(), (1,0))[:-1]
    prev_bid_top = bid_qty_top.select(pl.col('curBidTop')).to_numpy().T
    prev_bid_top = np.pad(prev_bid_top.ravel(), (1,0))[:-1]
    bid_qty_top = bid_qty_top.with_columns(prevBidQty = prev_bid_qty, prevBidTop = prev_bid_top)
    bid_qty_top = bid_qty_top.with_columns(pl.when(pl.col('curBidQty') > pl.col('prevBidQty')).then(1).otherwise(0).alias('Add'))
    bid_qty_top = bid_qty_top.with_columns(bid_qty_top.select(pl.cum_sum('Add').alias('bidSideAdd'))).drop('Add')
    bid_qty_top = bid_qty_top.with_columns(pl.when(pl.col('curBidQty') < pl.col('prevBidQty')).then(1).otherwise(0).alias('Delete'))
    bid_qty_top = bid_qty_top.with_columns(bid_qty_top.select(pl.cum_sum('Delete').alias('bidSideDelete'))).drop('Delete')
    bid_qty_top = bid_qty_top.with_columns(pl.when(pl.col('curBidTop') < pl.col('prevBidTop')).then(1).otherwise(0).alias('Flip'))
    bid_qty_top = bid_qty_top.with_columns(bid_qty_top.select(pl.cum_sum('Flip').alias('bidSideFlip'))).drop('Flip')
    bid_qty_top = bid_qty_top.with_columns(pl.when((pl.col('curBidQty') != pl.col('prevBidQty')) | (pl.col('curBidTop') < pl.col('prevBidTop'))).then(1).otherwise(0).alias('Count'))
    bid_qty_top = bid_qty_top.with_columns(bid_qty_top.select(pl.cum_sum('Count').alias('bidSideCount'))).drop('Count')
    bid_qty_top = bid_qty_top.with_columns(((pl.col('bidSideAdd') - pl.col('bidSideDelete') - pl.col('bidSideFlip'))/pl.col('bidSideCount')**ratio).alias('bidBookV'))
    
    book_d = pl.DataFrame(bid_qty_top.select(pl.col(['timestamp', 'bidBookV'])))
    print(book_d)
    
    ask_qty_top = orderbook_ask.group_by('timestamp').agg(pl.col('quantity').sum(), pl.col('price').min()).sort('timestamp')
    ask_qty_top = ask_qty_top.rename({'quantity' : 'curAskQty', 'price' : 'curAskTop'})
    prev_ask_qty = ask_qty_top.select(pl.col('curAskQty')).to_numpy().T
    prev_ask_qty = np.pad(prev_ask_qty.ravel(), (1,0))[:-1]
    prev_ask_top = ask_qty_top.select(pl.col('curAskTop')).to_numpy().T
    prev_ask_top = np.pad(prev_ask_top.ravel(), (1,0))[:-1]
    ask_qty_top = ask_qty_top.with_columns(prevAskQty = prev_ask_qty, prevAskTop = prev_ask_top)
    

PATH = "./" 
  
def main():
    pl.Config(set_fmt_float = "full")
    global PATH
    orderbook = pl.read_csv(PATH + "book-2024-04-09-exchange-market.csv")
    mid_price = GetMidPrice(orderbook)
    mid_price_type = ['midprice', 'midprice_mean', 'midprice_mkt']
    features = mid_price
    ratios = np.round(np.random.rand(3), 2)
    for mid_type in mid_price_type:
        for ratio in ratios:
            book_imbalance = BookImbalance(orderbook, ratio, 1, mid_price, mid_type)
            features = pl.concat([features, book_imbalance], how = 'align')

    BookDelta(orderbook, 0.2)
    
    # features.write_csv(PATH + "2024-04-09-exchange-market-feature.csv")


if __name__ == '__main__':
    main()