import polars as pl
import numpy as np
from datetime import datetime
import sys

def main():
    pl.Config(set_fmt_float = "full")
    file_name = sys.argv[1]
    data = pl.read_csv(file_name)
    data = data.with_columns(data.select((pl.col('quantity') * pl.col('price')).alias('px')))
    data = data.with_columns(pl.when(pl.col('side') == 0).then(-1).otherwise(1).alias('type'))
    data = data.with_columns(data.select((pl.col('px') * pl.col('type') - pl.col('fee')).alias('tot_px')))
    data = data.with_columns(pl.cum_sum('tot_px').alias('PnL_per_trade'))
    trade_amount = data.select(pl.col('timestamp', 'PnL_per_trade'))
    
    timestamp_amount = data.select(pl.col('timestamp', 'tot_px'))
    timestamp_amount = timestamp_amount.group_by('timestamp').agg(pl.col('tot_px').sum()).sort('timestamp')
    timestamp_amount = timestamp_amount.rename({'tot_px' : 'PnL_per_timestamp'})
    
    
    timestamp_day = data.select(pl.col('timestamp')).to_numpy()
    pt = []
    for time in timestamp_day:
        tmp = datetime.strptime(time[0], "%Y-%m-%d %H:%M").date()
        pt.append(datetime.strftime(tmp, "%y-%m-%d"))
    data = data.with_columns(pl.Series(pt).alias('timestamp_date'))
    date_amount = data.group_by('timestamp_date').agg(pl.col('tot_px').sum()).sort('timestamp_date')
    
    prev_date_amount = date_amount.select(pl.col('tot_px')).to_numpy().T
    prev_date_amount = np.pad(prev_date_amount.ravel(), (1,0))[:-1]
    date_amount = date_amount.with_columns(prev_tot_px = prev_date_amount)
    
    date_amount = date_amount.with_columns((pl.col('tot_px') + pl.col('prev_tot_px')).alias('PnL_each_date'))
    date_amount = date_amount.with_columns((pl.cum_sum('tot_px')).alias('PnL_per_date'))
    date_amount = date_amount.select(pl.col('timestamp_date', 'PnL_each_date', 'PnL_per_date'))
    
    trade_amount.write_csv("PnL_per_trade_" + file_name)
    timestamp_amount.write_csv("PnL_per_timestamp_" + file_name)
    date_amount.write_csv("PnL_per_date_" + file_name)
    
    f = open("./PnL_score.csv", 'w')
    f.write(f"File Name : {file_name}, PnL score : {date_amount['PnL_per_date'][-1]:.1f}")
    f.close()
        
if __name__ == '__main__':
    main()