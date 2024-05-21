import pandas as pd
import polars as pl
import numpy as np
import glob

def GetMidPrice(orderbook):
    # bid/ask devide
    orderbook_bid = orderbook.filter(pl.col('type') == 0)  
    orderbook_ask = orderbook.filter(pl.col('type') == 1)  
    
    # bid/ask top price
    orderbook_bid_top = orderbook_bid.group_by('timestamp').first().sort('timestamp')   
    orderbook_ask_top = orderbook_ask.group_by('timestamp').first().sort('timestamp')   
    
    # bid/ask mean(average) price
    orderbook_bid_mean = orderbook_bid.group_by('timestamp').agg(pl.col('price').mean()).sort('timestamp')  
    orderbook_ask_mean = orderbook_ask.group_by('timestamp').agg(pl.col('price').mean()).sort('timestamp')  
    
    # initialize with timestamps
    mid_price = orderbook_ask_top.select(pl.col('timestamp')).sort('timestamp')     
    
    # calculate mid price
    mid = (orderbook_ask_top.select(pl.col('price')) + orderbook_bid_top.select(pl.col('price'))) * 0.5     
    mid_price = mid_price.with_columns(mid.select(pl.col('price'))).rename({'price' : 'midprice'})
    
    # calculate mid price mean
    mid_mean = (orderbook_ask_mean.select(pl.col('price')) + orderbook_bid_mean.select(pl.col('price'))) * 0.5      
    mid_price = mid_price.with_columns(mid_mean.select(pl.col('price'))).rename({'price' : 'midprice_mean'})
    
    # calculate mid price mkt
    mid_mkt = (orderbook_bid_top.select(pl.col('price')) * orderbook_ask_top.select(pl.col('quantity')) + \
            orderbook_ask_top.select(pl.col('price')) * orderbook_bid_top.select(pl.col('quantity'))) / \
            (orderbook_bid_top.select(pl.col('quantity')) + orderbook_ask_top.select(pl.col('quantity')))           
    mid_price = mid_price.with_columns(mid_mkt.select(pl.col('price'))).rename({'price' : 'midprice_mkt'})
    mid_price = mid_price.with_columns(mid_price.select(pl.col('midprice_mkt').round(3).alias("midprice_mkt_round"))).drop("midprice_mkt")
    mid_price = mid_price.rename({'midprice_mkt_round' : 'midprice_mkt'})
    
    return mid_price

def BookImbalance(orderbook, ratio, interval, midprice, mid_type):
    # bid/ask devide
    orderbook_bid = orderbook.filter(pl.col('type') == 0)   
    orderbook_ask = orderbook.filter(pl.col('type') == 1)   
    
    # bid/ask qty
    orderbook_bid = orderbook_bid.with_columns(orderbook_bid.select((pl.col('quantity') ** ratio).alias("qty")))        
    orderbook_ask = orderbook_ask.with_columns(orderbook_ask.select((pl.col('quantity') ** ratio).alias("qty")))    
    
    # bid/ask px    
    orderbook_bid = orderbook_bid.with_columns(orderbook_bid.select((pl.col('price')*pl.col("qty")).alias("px")))       
    orderbook_ask = orderbook_ask.with_columns(orderbook_ask.select((pl.col('price')*pl.col("qty")).alias("px")))       
    
    # bid/ask sum_qty, sum_px
    bid_qty_px = orderbook_bid.group_by('timestamp').agg(pl.col('qty').sum(), pl.col('px').sum()).sort('timestamp')     
    ask_qty_px = orderbook_ask.group_by('timestamp').agg(pl.col('qty').sum(), pl.col('px').sum()).sort('timestamp')     
     
    # calculate book price
    book_price = (ask_qty_px.select(pl.col('qty')) * bid_qty_px.select(pl.col('px')) / bid_qty_px.select(pl.col('qty')) + \
            bid_qty_px.select(pl.col('qty')) * ask_qty_px.select(pl.col('px')) / ask_qty_px.select(pl.col('qty'))) / \
            (ask_qty_px.select(pl.col('qty')) + bid_qty_px.select(pl.col('qty')))
    book_price = book_price.rename({'qty' : 'bookprice'})
    
    # initialize book imbalance
    book_imbalance = bid_qty_px.select(pl.col('timestamp')).sort('timestamp')
    
    # calculate book imbalance
    imbalance = (book_price.select(pl.col('bookprice')) - midprice.select(pl.col(mid_type))) / interval
    book_imbalance = book_imbalance.with_columns(imbalance.select(pl.col('bookprice').round(3)).rename({'bookprice' : 'book-imbalance-{:.1f}-5-{}-{}'.format(ratio, interval, mid_type)}))

    return book_imbalance

def BookDelta(orderbook, ratio, interval):
    # bid/ask devide
    orderbook_bid = orderbook.filter(pl.col('type') == 0)   
    orderbook_ask = orderbook.filter(pl.col('type') == 1)   
    
    # bid curBidQty, curBidTop
    bid_qty_top = orderbook_bid.group_by('timestamp').agg(pl.col('quantity').sum(), pl.col('price').max()).sort('timestamp')    
    bid_qty_top = bid_qty_top.rename({'quantity' : 'curBidQty', 'price' : 'curBidTop'})
    
    # add prev quantity, prev top price
    prev_bid_qty = bid_qty_top.select(pl.col('curBidQty')).to_numpy().T
    prev_bid_qty = np.pad(prev_bid_qty.ravel(), (1,0))[:-1]
    prev_bid_top = bid_qty_top.select(pl.col('curBidTop')).to_numpy().T
    prev_bid_top = np.pad(prev_bid_top.ravel(), (1,0))[:-1]
    bid_qty_top = bid_qty_top.with_columns(prevBidQty = prev_bid_qty, prevBidTop = prev_bid_top)
    
    # calculate bidBookD
    bid_qty_top = bid_qty_top.with_columns(pl.when(pl.col('curBidQty') > pl.col('prevBidQty')).then(1).otherwise(0).alias('Add'))
    bid_qty_top = bid_qty_top.with_columns(bid_qty_top.select(pl.cum_sum('Add').alias('bidSideAdd'))).drop('Add')
    bid_qty_top = bid_qty_top.with_columns(pl.when(pl.col('curBidQty') < pl.col('prevBidQty')).then(1).otherwise(0).alias('Delete'))
    bid_qty_top = bid_qty_top.with_columns(bid_qty_top.select(pl.cum_sum('Delete').alias('bidSideDelete'))).drop('Delete')
    bid_qty_top = bid_qty_top.with_columns(pl.when(pl.col('curBidTop') < pl.col('prevBidTop')).then(1).otherwise(0).alias('Flip'))
    bid_qty_top = bid_qty_top.with_columns(bid_qty_top.select(pl.cum_sum('Flip').alias('bidSideFlip'))).drop('Flip')
    bid_qty_top = bid_qty_top.with_columns(pl.when((pl.col('curBidQty') != pl.col('prevBidQty')) | (pl.col('curBidTop') < pl.col('prevBidTop'))).then(1).otherwise(0).alias('Count'))
    bid_qty_top = bid_qty_top.with_columns(bid_qty_top.select(pl.cum_sum('Count').alias('bidSideCount'))).drop('Count')
    bid_qty_top = bid_qty_top.with_columns(((pl.col('bidSideAdd') - pl.col('bidSideDelete') - pl.col('bidSideFlip'))/pl.col('bidSideCount')**ratio).alias('bidBookD'))
    
    book_d = pl.DataFrame(bid_qty_top.select(pl.col(['timestamp', 'bidBookD'])))

    # ask curBidQty, curBidTop
    ask_qty_top = orderbook_ask.group_by('timestamp').agg(pl.col('quantity').sum(), pl.col('price').min()).sort('timestamp')
    ask_qty_top = ask_qty_top.rename({'quantity' : 'curAskQty', 'price' : 'curAskTop'})
    
    # add prev quantity, prev top price
    prev_ask_qty = ask_qty_top.select(pl.col('curAskQty')).to_numpy().T
    prev_ask_qty = np.pad(prev_ask_qty.ravel(), (1,0))[:-1]
    prev_ask_top = ask_qty_top.select(pl.col('curAskTop')).to_numpy().T
    prev_ask_top = np.pad(prev_ask_top.ravel(), (1,0))[:-1]
    ask_qty_top = ask_qty_top.with_columns(prevAskQty = prev_ask_qty, prevAskTop = prev_ask_top)
    
    # calculate addBookD
    ask_qty_top = ask_qty_top.with_columns(pl.when(pl.col('curAskQty') > pl.col('prevAskQty')).then(1).otherwise(0).alias('Add'))
    ask_qty_top = ask_qty_top.with_columns(ask_qty_top.select(pl.cum_sum('Add').alias('askSideAdd'))).drop('Add')
    ask_qty_top = ask_qty_top.with_columns(pl.when(pl.col('curAskQty') < pl.col('prevAskQty')).then(1).otherwise(0).alias('Delete'))
    ask_qty_top = ask_qty_top.with_columns(ask_qty_top.select(pl.cum_sum('Delete').alias('askSideDelete'))).drop('Delete')
    ask_qty_top = ask_qty_top.with_columns(pl.when(pl.col('curAskTop') < pl.col('prevAskTop')).then(1).otherwise(0).alias('Flip'))
    ask_qty_top = ask_qty_top.with_columns(ask_qty_top.select(pl.cum_sum('Flip').alias('askSideFlip'))).drop('Flip')
    ask_qty_top = ask_qty_top.with_columns(pl.when((pl.col('curAskQty') != pl.col('prevAskQty')) | (pl.col('curAskTop') < pl.col('prevAskTop'))).then(1).otherwise(0).alias('Count'))
    ask_qty_top = ask_qty_top.with_columns(ask_qty_top.select(pl.cum_sum('Count').alias('askSideCount'))).drop('Count')
    ask_qty_top = ask_qty_top.with_columns(((pl.col('askSideAdd') - pl.col('askSideDelete') - pl.col('askSideFlip'))/pl.col('askSideCount')**ratio).alias('askBookD'))
    
    book_d = book_d.with_columns(ask_qty_top.select(pl.col('askBookD')))
    
    # calculate Book delta
    book_d = book_d.with_columns((pl.col('bidBookD') + pl.col('askBookD')).alias('book-delta-{}-5-{}'.format(ratio, interval)))
    
    return book_d.select(pl.col(['timestamp', 'book-delta-{:.1f}-5-{}'.format(ratio, interval)]))

  
def main():
    PATH = "./" 
    pl.Config(set_fmt_float = "full")

    # from data 'PATH' bring all orderbook dataset names
    orderbook_list = glob.glob(PATH + 'book*.csv')
    
    # for each orderbook dataset
    for orderbook_name in orderbook_list:
        # load orderbook
        orderbook = pl.read_csv(orderbook_name)

        # get mid price
        mid_price = GetMidPrice(orderbook)
        mid_price_type = ['midprice', 'midprice_mean', 'midprice_mkt']
        
        # initialize features using mid price
        features = mid_price
        
        # calculate book imabalance with (random) ratios and each mid price
        ratios = np.round(np.random.rand(3), 2) # ratios = [0.1, 0.2, 0.5]
        for mid_type in mid_price_type:
            for ratio in ratios:
                book_imbalance = BookImbalance(orderbook, ratio, 1, mid_price, mid_type)
                features = pl.concat([features, book_imbalance], how = 'align')
        
        # calculate book imabalance with ratio
        ratio_d = [0.1, 0.2, 0.5]
        for ratio in ratio_d:
            book_delta = BookDelta(orderbook, ratio, 1)
            features = pl.concat([features, book_delta], how = 'align')

        # export features to csv file
        features.write_csv(orderbook_name[7:-4] + "-feature.csv")

if __name__ == '__main__':
    main()