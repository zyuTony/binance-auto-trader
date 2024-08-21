import pandas as pd 
from datetime import datetime, timezone
from binance.client import Client
from binance.client import Client 
from binance.enums import *
import psycopg2
from psycopg2 import OperationalError
    
# pairs selection from sql criteria
IB_MIN_RECENT_COINT = 0.75
IB_MIN_R_SQUARED = 0.65
IB_MIN_POTENTIAL_WIN_PCT = 0.01

# trade params
IB_TOTAL_USDT_PER_TRADE = 10
IB_BB_BAND_WINDOW = 20 
IB_BB_SIGNAL_STD_MULT = 1.8
IB_BB_STOPLOSS_STD_MULT = 2.8

current_date = datetime.now().strftime("%Y-%m-%d")
ib_strat_csv_file = f'/home/ubuntu/binance_pair_trader/data/ib_strat_df_{current_date}.csv'
ib_order_csv_file = '/home/ubuntu/binance_pair_trader/data/ib_order_df.csv'

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

def create_ib_latest_trades_table(conn):
    cursor = conn.cursor()
    try:
        create_table_query = """
        CREATE TABLE IF NOT EXISTS ib_latest_trades (
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
        print(f"ib_latest_trades created successfully.")
    except Exception as e:
        print(f"Failed to create table: {str(e)}")
        conn.rollback()
    finally:
        cursor.close()

def send_ib_executed_orders_to_sql(conn, order):
      cursor = conn.cursor() 
      
      # Extract data from the trade object
      execution = order.fills[0].execution   
      date = execution.time.strftime('%Y-%m-%d %H:%M:%S')
      symbol = order.contract.symbol
      action = order.order.action
      amt = order.order.totalQuantity
      price = order.orderStatus.avgFillPrice
      dollar_amt = amt * price
      
      cursor.execute(
         """
         INSERT INTO ib_latest_trades (date, symbol, action, dollar_amt, price, amt) 
         VALUES (%s, %s, %s, %s, %s, %s)
         ON CONFLICT (date, symbol)
            DO NOTHING
         """,
         (date, symbol, action, dollar_amt, price, amt)
      )
      conn.commit()
      print("Order details written to SQL table ib_latest_trades.")
    
    
def ib_candle_transformation(candle_data):
    output = []
    for entry in candle_data:
        date = entry.date.strftime('%Y-%m-%d %H:%M:%S')
        open_price = entry.open
        high_price = entry.high
        low_price = entry.low
        close_price = entry.close
        volume = entry.volume
        output.append([date, open_price, high_price, low_price, close_price, volume])

    output_df = pd.DataFrame(output, columns=['date', 'open', 'high', 'low', 'close', 'volume'])
    output_df['date'] = pd.to_datetime(output_df['date'])
    output_df['open'] = output_df['open'].astype(float)
    output_df['high'] = output_df['high'].astype(float)
    output_df['low'] = output_df['low'].astype(float)
    output_df['close'] = output_df['close'].astype(float)
    output_df['volume'] = output_df['volume'].astype(float)

    return output_df

def get_ib_data(ib_client, contract): 
   
      daily_candle = ib_client.reqHistoricalData(
      contract,
      endDateTime='',
      durationStr='120 D',
      barSizeSetting='1 day',
      whatToShow='TRADES',
      useRTH=True,
      formatDate=1)
   
      minute_candle = ib_client.reqHistoricalData(
      contract,
      endDateTime='',
      durationStr='1 D',
      barSizeSetting='1 min',
      whatToShow='TRADES',
      useRTH=True,
      formatDate=1)
      
      minute_data = ib_candle_transformation(minute_candle)
      daily_data = ib_candle_transformation(daily_candle)
      return minute_data, daily_data

def ib_pairs_order_to_pd_df(pair_trade_status, ols_coeff, ols_constant, long_order, short_order, symbol_Y, symbol_X):

   short_order_tranx_time_str = short_order.fills[0].execution.time.strftime('%Y-%m-%d %H:%M:%S')   
   long_order_tranx_time_str = long_order.fills[0].execution.time.strftime('%Y-%m-%d %H:%M:%S')   
   
   short_order_shares = short_order_tranx_time_str.order.totalQuantity
   long_order_shares = long_order_tranx_time_str.order.totalQuantity
   
   short_order_price = short_order.orderStatus.avgFillPrice
   long_order_price = long_order.orderStatus.avgFillPrice
   
   short_order_usd_amt = short_order_price*short_order_shares
   long_order_usd_amt = long_order_price*long_order_shares
    
   return pd.DataFrame([{
      'pair_trade_status': pair_trade_status,
      'symbol_Y': symbol_Y,
      'symbol_X': symbol_X,
      'long_symbol': long_order.contract.symbol,
      'long_time': long_order_tranx_time_str,
      'long_side': long_order.order.action,
      'long_quantity': long_order_shares,
      'long_usd_amt': long_order_usd_amt,
      'long_status': long_order.orderStatus.status,
      'long_orderId': long_order.order.orderId,
      'long_clientOrderId': long_order.order.clientId,
      
      'short_symbol': short_order.contract.symbol,
      'short_time': short_order_tranx_time_str,
      'short_side': short_order.order.action,
      'short_quantity': short_order_shares,
      'short_usd_amt': short_order_usd_amt,
      'short_status': short_order.orderStatus.status,
      'short_orderId': short_order.order.orderId,
      'short_clientOrderId': short_order.order.clientId,
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

 