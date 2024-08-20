import pandas as pd
from datetime import datetime
from binance.client import Client
from datetime import datetime, timezone
from binance.client import Client 
from binance.enums import *
import psycopg2
from psycopg2 import OperationalError

# pairs selection from sql criteria
MIN_RECENT_COINT = 0.75
MIN_R_SQUARED = 0.65
MIN_POTENTIAL_WIN_PCT = 0.01

# trade params
TOTAL_USDT_PER_TRADE = 50
BB_BAND_WINDOW = 20 
BB_SIGNAL_STD_MULT = 1.8
BB_STOPLOSS_STD_MULT = 2.8

# file location
strat_csv_file = '/home/ec2-user/binance_pair_trader/strat_df.csv'
order_csv_file = '/home/ec2-user/binance_pair_trader/order_df.csv'

def connect_to_db(DB_NAME, DB_HOST, DB_USERNAME, DB_PASSWORD):
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USERNAME,
            password=DB_PASSWORD)
        print(f"Connected to {DB_NAME}!")
        return conn
    except OperationalError as e:
        print(f"{e}")
        return None

def create_latest_trades_table(conn):
    cursor = conn.cursor()
    try:
        create_table_query = """
        CREATE TABLE IF NOT EXISTS latest_trades (
            date TIMESTAMPTZ NOT NULL,
            symbol VARCHAR(10) NOT NULL,
            action VARCHAR(10) NOT NULL,
            dollar_amt NUMERIC NOT NULL,
            price NUMERIC NOT NULL,
            amt NUMERIC NOT NULL,
            UNIQUE (date, symbol)
        );
        """
        cursor.execute(create_table_query)
        conn.commit()
        print(f"latest_trades created successfully.")
    except Exception as e:
        print(f"Failed to create table: {str(e)}")
        conn.rollback()
    finally:
        cursor.close()


def send_executed_orders_to_sql(conn, order):
      cursor = conn.cursor() 
      date = datetime.fromtimestamp(order['transactTime'] / 1000).strftime('%Y-%m-%d %H:%M:%S')
      symbol = order['symbol']
      action = order['side']
      amt = float(order['executedQty'])
      dollar_amt = float(order['cummulativeQuoteQty'])
      price = dollar_amt/amt
      
      cursor.execute(
         """
         INSERT INTO latest_trades (date, symbol, action, dollar_amt, price, amt) 
         VALUES %s
         ON CONFLICT (date, symbol)
            DO NOTHING
         """,
         (date, symbol, action, dollar_amt, price, amt)
      )
      conn.commit()
      print("Order details written to SQL table latest_trades.")
    
    
def candle_transformation(candle):
      output = []
      for entry in candle:
         date = datetime.fromtimestamp(entry[0] / 1000, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
         open_price = entry[1]
         high_price = entry[2]
         low_price = entry[3]
         close_price = entry[4]
         volume = entry[5]
         output.append([date, open_price, high_price, low_price, close_price, volume])

      output_df = pd.DataFrame(output, columns=['date', 'open', 'high', 'low', 'close', 'volume'])
      output_df['date'] = pd.to_datetime(output_df['date'])
      output_df['open'] = output_df['open'].astype(float)
      output_df['high'] = output_df['high'].astype(float)
      output_df['low'] = output_df['low'].astype(float)
      output_df['close'] = output_df['close'].astype(float)
      output_df['volume'] = output_df['volume'].astype(float)

      return output_df

def get_bn_data(client, symbol): 
      minute_candles = client.get_klines(symbol=symbol, interval=Client.KLINE_INTERVAL_1MINUTE)
      minute_data = candle_transformation(minute_candles)
 
      daily_candle = client.get_historical_klines(symbol, Client.KLINE_INTERVAL_1DAY, "120 day ago UTC")
      daily_data = candle_transformation(daily_candle)
      return minute_data, daily_data

def get_tick_size(curr_price_Y, curr_price_X):
   if curr_price_Y < 5:
      y_tick_size = 1
   elif curr_price_Y >=5 and curr_price_Y < 100:
      y_tick_size = 0.01
   elif curr_price_Y >=100 and curr_price_Y < 2000:
      y_tick_size = 0.001  
   elif curr_price_Y >=1000 and curr_price_Y < 10000:
      y_tick_size = 0.0001 
   elif curr_price_Y >=10000:
      y_tick_size = 0.00001 

   if curr_price_X < 5:
      x_tick_size = 1
   elif curr_price_X >=5 and curr_price_X < 100:
      x_tick_size = 0.01
   elif curr_price_X >=100 and curr_price_X < 2000:
      x_tick_size = 0.001  
   elif curr_price_X >=1000 and curr_price_X < 10000:
      x_tick_size = 0.0001 
   elif curr_price_X >=10000:
      x_tick_size = 0.00001 
   return y_tick_size, x_tick_size

def pairs_order_to_pd_df(pair_trade_status, ols_coeff, ols_constant, long_order, short_order, short_loan, symbol_Y, symbol_X):
   
   short_order_tranx_time_str = datetime.fromtimestamp(short_order['transactTime'] / 1000).strftime('%Y-%m-%d %H:%M:%S')
   long_order_tranx_time_str = datetime.fromtimestamp(long_order['transactTime'] / 1000).strftime('%Y-%m-%d %H:%M:%S')
   
   short_order_coins_quantity = float(short_order['executedQty'])
   long_order_coins_quantity = float(long_order['executedQty'])
   
   short_order_usdt_amt = float(short_order['cummulativeQuoteQty'])
   long_order_usdt_amt = float(long_order['cummulativeQuoteQty'])
    
   return pd.DataFrame([{
      'pair_trade_status': pair_trade_status,
      'symbol_Y': symbol_Y,
      'symbol_X': symbol_X,
      'long_symbol': long_order['symbol'],
      'long_time': long_order_tranx_time_str,
      'long_side': long_order['side'],
      'long_quantity': long_order_coins_quantity,
      'long_usdt_amt': long_order_usdt_amt,
      'long_status': long_order['status'],
      'long_orderId': str(long_order['orderId']),
      'long_clientOrderId': str(long_order['clientOrderId']),
      
      'short_symbol': short_order['symbol'],
      'short_time': short_order_tranx_time_str,
      'short_side': short_order['side'],
      'short_quantity': short_order_coins_quantity,
      'short_usdt_amt': short_order_usdt_amt,
      'short_status': short_order['status'],
      'short_loanId': str(short_loan['tranId']),
      'short_orderId': str(short_order['orderId']),
      'short_clientOrderId': str(short_order['clientOrderId']),
      'ols_coeff': ols_coeff,
      'ols_constant': ols_constant
   }])

def calculate_bollinger_bands(df, window, std_dev, stop_loss_std_dev, col_name='spread'):
    df['rolling_mean'] = df[col_name].rolling(window=window).mean()
    df['rolling_std'] = df[col_name].rolling(window=window).std()
    df['upper_band'] = df['rolling_mean'] + (df['rolling_std'] * std_dev)
    df['lower_band'] = df['rolling_mean'] - (df['rolling_std'] * std_dev)
    df['upper_stop_loss'] = df['rolling_mean'] + (df['rolling_std'] * stop_loss_std_dev)
    df['lower_stop_loss'] = df['rolling_mean'] - (df['rolling_std'] * stop_loss_std_dev)
    return df

def calculate_spread(df1, df2, ols_coeff, ols_constant=0):
    df1 = df1.set_index('date')
    df2 = df2.set_index('date')
    spread = df1['close'] - ols_constant - ols_coeff * df2['close']
    
    result_df = pd.DataFrame({
        'date': df1.index,
        'spread': spread
    }).reset_index(drop=True)
    return result_df 

def order_to_pd_df(order, symbol_Y, symbol_X, margin_type):
   transact_time_dt = datetime.fromtimestamp(order['transactTime'] / 1000)
   transact_time_str = transact_time_dt.strftime('%Y-%m-%d %H:%M:%S')
   coins_quantity = float(order['executedQty'])
   usdt_amt = float(order['cummulativeQuoteQty'])
   if order['side'] == 'SELL':
      coins_quantity = -coins_quantity
   if order['side'] == 'BUY':
      usdt_amt = -usdt_amt
        
   return  pd.DataFrame([{
      'symbol_Y': symbol_Y,
      'symbol_X': symbol_X,
      'margin': margin_type,
      'side': order['side'],
      'symbol': order['symbol'],
      'datetime': transact_time_str,
      'coins_quantity': coins_quantity,
      'usdt_amt': usdt_amt,
      'status': order['status'],
      'type': order['type'],
      'orderId': order['orderId'],
      'clientOrderId': order['clientOrderId']
   }])

# KLINE_INTERVAL_1MINUTE 
# KLINE_INTERVAL_3MINUTE 
# KLINE_INTERVAL_5MINUTE 
# KLINE_INTERVAL_15MINUTE 
# KLINE_INTERVAL_30MINUTE 
# KLINE_INTERVAL_1HOUR 
# KLINE_INTERVAL_2HOUR 
# KLINE_INTERVAL_4HOUR 
# KLINE_INTERVAL_6HOUR 
# KLINE_INTERVAL_8HOUR 
# KLINE_INTERVAL_12HOUR 
# KLINE_INTERVAL_1DAY 