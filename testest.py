import os
from dotenv import load_dotenv
import pandas as pd
from binance.client import Client
from binance.helpers import round_step_size
from binance.enums import *
from utils.strat_utils import *
from utils.trading_utils import *
import requests
import sys

load_dotenv()
api_key = os.getenv('BINANCE_API')
api_secret = os.getenv('BINANCE_SECRET')
avan_api_key = os.getenv('ALPHA_VANTAGE_PREM_API') 

DB_USERNAME = os.getenv('RDS_USERNAME')
DB_PASSWORD = os.getenv('RDS_PASSWORD')
DB_HOST = os.getenv('RDS_ENDPOINT')
DB_NAME = 'financial_data'

import pandas as pd
import numpy as np

# binance data
client = Client(api_key, api_secret)
min_df_chart, day_df_chart = get_bn_data(client, 'BTCUSDT')
print(min_df_chart, day_df_chart)



# # BN profit loss pair trade
# df = pd.read_csv('./data/order_df.csv')

# # Convert time columns to datetime
# df['long_time'] = pd.to_datetime(df['long_time'])
# df['short_time'] = pd.to_datetime(df['short_time'])

# # Function to calculate profit/loss for a pair of trades
# def calculate_pnl(open_trade, close_trade):
#     long_pnl = close_trade['long_usdt_amt'] - open_trade['long_usdt_amt']
#     short_pnl = open_trade['short_usdt_amt'] - close_trade['short_usdt_amt']
#     return long_pnl + short_pnl

# # Function to calculate total traded volume
# def calculate_volume(open_trade, close_trade):
#     open_volume = open_trade['long_usdt_amt'] + open_trade['short_usdt_amt']
#     close_volume = close_trade['long_usdt_amt'] + close_trade['short_usdt_amt']
#     return open_volume 

# # Initialize list to store results
# results = []

# # Filter for non-OPEN trades
# open_trades = df[df['pair_trade_status'].isin(['LOWER_STOPPED', 'PROFIT_CLOSED', 'UPPER_STOPPED'])]

# for _, open_trade in open_trades.iterrows():
#     # Find corresponding closing trade
#     close_trade = df[(df['pair_trade_status'] == 'CLOSING_TRADE') & 
#                      (df['symbol_Y'] == open_trade['symbol_Y']) & 
#                      (df['symbol_X'] == open_trade['symbol_X']) &
#                      (df['long_time'] > open_trade['long_time'])]
    
#     if not close_trade.empty:
#         close_trade = close_trade.iloc[0]
#         pnl = calculate_pnl(open_trade, close_trade)
#         volume = calculate_volume(open_trade, close_trade)
        
#         results.append({
#             'symbol_Y': open_trade['symbol_Y'],
#             'symbol_X': open_trade['symbol_X'],
#             'open_time': open_trade['long_time'],
#             'close_time': close_trade['long_time'],
#             'pnl': pnl,
#             'volume': volume
#         })
  
# # Convert results to DataFrame
# results_df = pd.DataFrame(results)

# # Calculate total PNL and total traded volume
# total_pnl = results_df['pnl'].sum()
# total_volume = results_df['volume'].sum()

# print(results_df)
# print(f"\nTotal PNL: {total_pnl:.2f} USDT")
# print(f"Total Traded Volume: {total_volume:.2f} USDT")
 