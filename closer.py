import os
from dotenv import load_dotenv  
import pandas as pd
from binance.client import Client 
from binance.helpers import round_step_size
from binance.enums import *
from utils.trading_utils import *
import sys

load_dotenv()
api_key = os.getenv('BINANCE_API')  
api_secret = os.getenv('BINANCE_SECRET')  
DB_USERNAME = os.getenv('RDS_USERNAME') 
DB_PASSWORD = os.getenv('RDS_PASSWORD') 
DB_HOST = os.getenv('RDS_ENDPOINT') 
DB_NAME = 'financial_data'


# must have existing orders to track
if os.path.exists(order_csv_file):
    orders_df = pd.read_csv(order_csv_file)
    if orders_df.empty:
      print('no existing order. order df empty. exit')
      sys.exit()
else:
   print('no existing order. no order df. exit')
   sys.exit()
    
new_orders_df = pd.DataFrame() 
client = Client(api_key, api_secret)
for index, row in orders_df.iterrows():
   if row['pair_trade_status'] == 'OPEN':
      symbol_Y = row['symbol_Y']
      symbol_X = row['symbol_X']
      long_symbol = row['long_symbol']
      short_symbol = row['short_symbol']
      ols_coeff = row['ols_coeff'] 
      ols_constant = row['ols_constant'] 
      # check closing condition for all pairs
      long_minute_data, long_daily_data = get_bn_data(client, long_symbol)
      short_minute_data, short_daily_data = get_bn_data(client, short_symbol)
      curr_price_long = round_step_size(long_minute_data['close'].iloc[-1], 0.00001)
      curr_price_short = round_step_size(short_minute_data['close'].iloc[-1], 0.00001)
      long_tick_size, short_tick_size = get_tick_size(curr_price_long, curr_price_short)
      
      minute_spread = calculate_spread(long_minute_data, short_minute_data, ols_coeff, ols_constant)
      daily_spread_range = calculate_bollinger_bands(calculate_spread(long_daily_data, short_daily_data, ols_coeff, ols_constant), BB_BAND_WINDOW, BB_SIGNAL_STD_MULT, BB_STOPLOSS_STD_MULT).drop(columns=['spread'])

      minute_spread['date_only'] = minute_spread['date'].dt.date
      min_spread_w_day_band = pd.merge(minute_spread, daily_spread_range, left_on='date_only', right_on=daily_spread_range['date'].dt.date, how='left')
      
      # whether revered back to mean
      latest_spread = min_spread_w_day_band['spread'].iloc[-1] 
      latest_mean = min_spread_w_day_band['rolling_mean'].iloc[-1] 
      prev_spread = min_spread_w_day_band['spread'].iloc[-2] 
      prev_mean = min_spread_w_day_band['rolling_mean'].iloc[-2]
      
      latest_diff = latest_spread - latest_mean
      prev_diff = prev_spread - prev_mean  
      
      # whether stop loss reached
      # - check if spread gone to stop loss
      latest_upper_stop_loss = min_spread_w_day_band['upper_stop_loss'].iloc[-1] 
      latest_lower_stop_loss = min_spread_w_day_band['lower_stop_loss'].iloc[-1] 
      
      # crossover detected
      if latest_diff * prev_diff <= 0 or (latest_spread >= latest_upper_stop_loss) or (latest_spread <= latest_lower_stop_loss): 
         try:
            if latest_diff * prev_diff <= 0:
               print(f"Crossover detected between {symbol_Y}X{symbol_X}.")
               orders_df.at[index, 'pair_trade_status'] = 'PROFIT_CLOSED'
            if (latest_spread >= latest_upper_stop_loss):
               print(f"Upper Stop loss band reached between {symbol_Y}X{symbol_X}.")
               orders_df.at[index, 'pair_trade_status'] = 'UPPER_STOPPED'
            if (latest_spread <= latest_lower_stop_loss):        
               print(f"Lower Stop loss band reached between {symbol_Y}X{symbol_X}.")
               orders_df.at[index, 'pair_trade_status'] = 'LOWER_STOPPED'
               
            ## close long
            # get asset balance
            balance = client.get_asset_balance(asset=long_symbol.replace('USDT', ''))
            symbol_to_sell = balance['asset']
            coin_amt_to_sell = round_step_size(balance['free'], long_tick_size)
            # sell all
            close_long_order = client.order_market_sell(symbol=long_symbol, quantity=coin_amt_to_sell)
            print(f'Closed long order for {long_symbol}. Sold {coin_amt_to_sell} of them.')
            
            ## close short
            loan_detail = client.get_margin_loan_details(asset=short_symbol.replace('USDT', ''), txId=str(row['short_loanId']))
            coin_amt_to_repay = round_step_size(loan_detail['rows'][0]['principal'], short_tick_size) 
            # buy owed amt and repay loan
            short_repurchase = client.create_margin_order(symbol=short_symbol, side=SIDE_BUY,type=ORDER_TYPE_MARKET, quantity=coin_amt_to_repay)
            close_short_loan = client.repay_margin_loan(asset=short_symbol.replace('USDT', ''), amount=str(coin_amt_to_repay))
            print(f'Closed short order for {short_symbol}. Repaid {coin_amt_to_repay} of them.')
      
            new_orders_df = pd.concat([new_orders_df, pairs_order_to_pd_df("CLOSING_TRADE", ols_coeff, ols_constant, close_long_order, short_repurchase, close_short_loan, symbol_Y, symbol_X)])
            
            conn = connect_to_db(DB_NAME, DB_HOST, DB_USERNAME, DB_PASSWORD)
            send_executed_orders_to_sql(conn, close_long_order)
            send_executed_orders_to_sql(conn, short_repurchase)
         except Exception as e:
            print(f"An error occurred executing orders: {str(e)}")
            continue
      else:
         print(f'{symbol_Y}X{symbol_X} spread is NORMAL at {datetime.now()}.')

# write to orders_df
orders_df = pd.concat([orders_df, new_orders_df])
orders_df.to_csv(order_csv_file, index=False)
print('Went through all orders :)')
 
 