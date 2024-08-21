import os
import math
from dotenv import load_dotenv
import pandas as pd
from datetime import datetime, timezone
 
from utils.ib_utils import *
import warnings
from ib_insync import *

warnings.filterwarnings(
    "ignore",
    message="pandas only supports SQLAlchemy connectable")

load_dotenv()
DB_USERNAME = os.getenv('RDS_USERNAME')
DB_PASSWORD = os.getenv('RDS_PASSWORD')
DB_HOST = os.getenv('RDS_ENDPOINT')
DB_NAME = 'financial_data'

ib = IB()
ib.connect('127.0.0.1', 7496, clientId=2)

conn = connect_to_db(DB_NAME, DB_HOST, DB_USERNAME, DB_PASSWORD)
create_ib_latest_trades_table(conn)

if os.path.exists(ib_order_csv_file):
    orders_df = pd.read_csv(ib_order_csv_file)
    if orders_df.empty:
        orders_df = pd.DataFrame()
else:
    orders_df = pd.DataFrame()
 
for i in range(1):
    symbol_Y = 'LCID'
    symbol_X = 'LCID'
    ols_coeff = 1
    ols_constant = 0
    print(
        f"\n--- Checking {symbol_Y} X {symbol_X} - coeff:{round(ols_coeff,2)} constant:{round(ols_constant,2)}")

 
    '''Monitoring for opens'''
    # 1. connect get latest real time data.
    try:
        contract_Y = Stock(symbol_Y, 'SMART', 'USD')
        Y_minute_data, Y_daily_data = get_ib_data(ib, contract_Y)
    except Exception as e:
        print(f"No Data for {symbol_Y} on Binance: {str(e)}")
        continue
    
    try:
        contract_Y = Stock(symbol_Y, 'SMART', 'USD')
        X_minute_data, X_daily_data = get_ib_data(ib, contract_Y)
    except Exception as e:
        print(f"No Data for {symbol_X} on Binance: {str(e)}")
        continue
 
 
    # 2. calculate spread.
    minute_spread = calculate_spread(
        Y_minute_data,
        X_minute_data,
        ols_coeff,
        ols_constant)
    daily_spread_range = calculate_bollinger_bands(
        calculate_spread(
            Y_daily_data,
            X_daily_data,
            ols_coeff,
            ols_constant),
        IB_BB_BAND_WINDOW,
        IB_BB_SIGNAL_STD_MULT,
        IB_BB_STOPLOSS_STD_MULT).drop(
            columns=['spread'])

    minute_spread['date_only'] = minute_spread['date'].dt.date
    min_spread_w_day_band = pd.merge(
        minute_spread,
        daily_spread_range,
        left_on='date_only',
        right_on=daily_spread_range['date'].dt.date,
        how='left')
    min_spread_w_day_band = min_spread_w_day_band.drop(
        columns=['date_only', 'date_y'])
    min_spread_w_day_band.rename(columns={'date_x': 'date'}, inplace=True)

    # 3. trade based on the spread. long x and short y.
    latest_min = min_spread_w_day_band.iloc[-1]
    print(
        f"current:{round(latest_min['spread'], 2)} upper:{round(latest_min['upper_band'], 2)} lower:{round(latest_min['lower_band'], 2)}")

    if latest_min['spread'] > latest_min['upper_band']:
        strat = 'short Y long X'
    elif latest_min['spread'] < latest_min['lower_band']:
        strat = 'long Y short X'
    else:
        strat = 'stand_by'

    latest_strat = pd.DataFrame([{'date': latest_min['date'],
                                  'symbol_Y': symbol_Y,
                                  'symbol_X': symbol_X,
                                  'strategy': strat}])
    if not os.path.exists(ib_strat_csv_file):
        latest_strat.to_csv(ib_strat_csv_file, mode='w', header=True, index=False)
    else:
        latest_strat.to_csv(
            ib_strat_csv_file,
            mode='a',
            header=False,
            index=False)

    '''Execute trade at prime condition'''
    if True:
        print(f"Executing a trade for {symbol_Y}X{symbol_X}...")
        symbol_Y = latest_strat['symbol_Y'].iloc[-1]
        symbol_X = latest_strat['symbol_X'].iloc[-1]

        # determine order size with formula
        curr_price_Y = Y_minute_data['close'].iloc[-1]
        curr_price_X = X_minute_data['close'].iloc[-1]

        # if single share price higher than per trade $, dont execute
        if curr_price_Y > IB_TOTAL_USDT_PER_TRADE/2:
            print(f'{symbol_Y} at ${curr_price_Y} too expensive. skip this trade')
            continue
        if curr_price_X > IB_TOTAL_USDT_PER_TRADE/2:
            print(f'{symbol_X} at ${curr_price_X} too expensive. skip this trade')
            continue
        
        amt_Y = IB_TOTAL_USDT_PER_TRADE / (curr_price_X * ols_coeff + curr_price_Y)
        amt_X = ols_coeff * amt_Y
        amt_Y = math.ceil(amt_Y)
        amt_X = math.ceil(amt_X)
        
        usdt_on_Y = amt_Y * curr_price_Y
        usdt_on_X = amt_X * curr_price_X
        
        print("curr_price_Y, amt_Y, curr_price_X, amt_X, usdt_on_Y, usdt_on_X")
        print(curr_price_Y, amt_Y, curr_price_X, amt_X, usdt_on_Y, usdt_on_X)
        if True:
            # long Y
            long_order_info =  MarketOrder('BUY', amt_Y)
            long_order = ib.placeOrder(contract_Y, long_order_info)
            ib.sleep(2)
            print(long_order_info)
            print(long_order)
            print(long_order.log)
            print(f'longed {symbol_Y}. bought {amt_Y} shares of ${amt_Y*curr_price_Y}')
            send_ib_executed_orders_to_sql(conn, long_order)

            # short X
            # short_order_info =  MarketOrder('SELL', amt_Y)
            # short_order = ib.placeOrder(contract_Y, short_order_info)
            # print(
            #     f'shorted {symbol_X}. short sold {amt_X} shares of ${amt_X*curr_price_X}')
            
            # print(short_order_info)
            # print(short_order)
            # send_executed_orders_to_sql(conn, short_order)
 
ib.disconnect()
