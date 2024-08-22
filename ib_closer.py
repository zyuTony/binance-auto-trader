import os
from dotenv import load_dotenv
import pandas as pd
from utils.ib_utils import *
import sys
from ib_insync import *

load_dotenv()
api_key = os.getenv('BINANCE_API')
api_secret = os.getenv('BINANCE_SECRET')
DB_USERNAME = os.getenv('RDS_USERNAME')
DB_PASSWORD = os.getenv('RDS_PASSWORD')
DB_HOST = os.getenv('RDS_ENDPOINT')
DB_NAME = 'financial_data'


# must have existing orders to track
if os.path.exists(ib_order_csv_file):
    orders_df = pd.read_csv(ib_order_csv_file)
    if orders_df.empty:
        print('no existing order. order df empty. exit')
        sys.exit()
else:
    print('no existing order. no order df. exit')
    sys.exit()

new_orders_df = pd.DataFrame()
ib = IB()
ib.connect('127.0.0.1', 7496, clientId=1)
for index, row in orders_df.iterrows():
    if row['pair_trade_status'] == 'OPEN':
        symbol_Y = row['symbol_Y']
        symbol_X = row['symbol_X']
        long_symbol = row['long_symbol']
        short_symbol = row['short_symbol']
        ols_coeff = row['ols_coeff']
        ols_constant = row['ols_constant']
        
        # check closing condition for all pairs
        contract_long = Stock(long_symbol, 'SMART', 'USD')
        long_minute_data, long_daily_data = get_ib_data(ib, contract_long)
        contract_short = Stock(short_symbol, 'SMART', 'USD')
        short_minute_data, short_daily_data = get_ib_data(ib, contract_short)
        curr_price_long = long_minute_data['close'].iloc[-1]
        curr_price_short = short_minute_data['close'].iloc[-1]

        minute_spread = calculate_spread(
            long_minute_data,
            short_minute_data,
            ols_coeff,
            ols_constant)
        daily_spread_range = calculate_bollinger_bands(
            calculate_spread(
                long_daily_data,
                short_daily_data,
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

        # whether revered back to mean
        latest_min = min_spread_w_day_band.iloc[-1]
        latest_spread = min_spread_w_day_band['spread'].iloc[-1]
        latest_mean = min_spread_w_day_band['rolling_mean'].iloc[-1]
        prev_spread = min_spread_w_day_band['spread'].iloc[-2]
        prev_mean = min_spread_w_day_band['rolling_mean'].iloc[-2]

        latest_diff = latest_spread - latest_mean
        prev_diff = prev_spread - prev_mean

        # whether stop loss reached
        latest_upper_stop_loss = min_spread_w_day_band['upper_stop_loss'].iloc[-1]
        latest_lower_stop_loss = min_spread_w_day_band['lower_stop_loss'].iloc[-1]

        # crossover detected
        if latest_diff * prev_diff <= 0 or (
                latest_spread >= latest_upper_stop_loss) or (
                latest_spread <= latest_lower_stop_loss):
            try:
                if latest_diff * prev_diff <= 0:
                    print(
                        f"Crossover detected between {symbol_Y} X {symbol_X}.")
                    orders_df.at[index, 'pair_trade_status'] = 'PROFIT_CLOSED'
                if (latest_spread >= latest_upper_stop_loss):
                    print(
                        f"Upper Stop loss band reached between {symbol_Y} X {symbol_X}.")
                    orders_df.at[index, 'pair_trade_status'] = 'UPPER_STOPPED'
                if (latest_spread <= latest_lower_stop_loss):
                    print(
                        f"Lower Stop loss band reached between {symbol_Y} X {symbol_X}.")
                    orders_df.at[index, 'pair_trade_status'] = 'LOWER_STOPPED'

                # close long
                long_amt = orders_df.at[index, 'long_quantity']
                close_long_order_info = MarketOrder('SELL', long_amt)
                close_long_order = ib.placeOrder(contract_long, close_long_order_info)

                print(
                    f'Closed long order for {long_symbol}. Sold {long_amt} of them.')

                # close short 
                short_amt = orders_df.at[index, 'short_quantity']
                close_short_order_info = MarketOrder('BUY', short_amt)
                close_short_order = ib.placeOrder(contract_short, close_short_order_info)

                print(
                    f'Closed short order for {short_symbol}. Sold {short_amt} of them.')

                new_orders_df = pd.concat([new_orders_df,
                                           ib_pairs_order_to_pd_df("CLOSING_TRADE",
                                                                latest_min,
                                                                ols_coeff,
                                                                ols_constant,
                                                                close_long_order,
                                                                close_short_order,
                                                                symbol_Y,
                                                                symbol_X)])

                conn = connect_to_db(
                    DB_NAME, DB_HOST, DB_USERNAME, DB_PASSWORD)
                send_ib_executed_orders_to_sql(conn, close_long_order)
                send_ib_executed_orders_to_sql(conn, close_short_order)
            except Exception as e:
                print(f"An error occurred executing orders: {str(e)}")
                continue
        else:
            print(f'{symbol_Y} X {symbol_X} spread is NORMAL at {datetime.now()}.')

# write to orders_df
orders_df = pd.concat([orders_df, new_orders_df])
orders_df.to_csv(ib_order_csv_file, index=False)
print('Went through all orders :)')
