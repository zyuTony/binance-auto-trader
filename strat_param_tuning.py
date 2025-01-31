from isolated_bn_data_db_updater.db_utils import *
from utils.tuning_utils import *
from utils.avan_utils import *

from utils.child_strats import *

import os
from dotenv import load_dotenv
from binance.enums import *

load_dotenv()
api_key = os.getenv('BINANCE_API')
api_secret = os.getenv('BINANCE_SECRET')
avan_api_key = os.getenv('ALPHA_VANTAGE_PREM_API') 

DB_USERNAME = os.getenv('RDS_USERNAME')
DB_PASSWORD = os.getenv('RDS_PASSWORD')
DB_HOST = os.getenv('RDS_ENDPOINT')
 DB_NAME = os.getenv('RDS_DB_NAME')

# # STONEWELL
# stonewell_param_ranges = {
#     'profit_threshold': [10],
#     'stoploss_threshold': [-0.05],
#     'max_high_retrace': [0.05],
#     'rsi_window': [7],
#     'rsi_window_2': [30],
#     'rsi_sma_window': [20],
#     'price_sma_window': [50],
#     'short_sma_window': [50],
#     'long_sma_window': [200],
#     'volume_short_sma_window': [14],
#     'volume_long_sma_window': [30], 
#     'atr_window': [10], 
#     'kc_sma_window': [20], 
#     'kc_mult': [2]
# }

# stonewell_tuner = strat_tuner(start_date='2021-01-01', 
#                   end_date='2024-01-01', 
#                   symbols=['ETH/BTC'], 
#                   strat_name=StoneWellStrategy, 
#                   param_ranges=stonewell_param_ranges,
#                   trade_df_timeframe='1hour', 
#                   indi_df_timeframe='1day')
# print(stonewell_tuner._get_data('ETH/BTC', '1day'))

# SIMPLE PRICE
simple_sma_param_ranges = {
    'profit_threshold': [10],
    'stoploss_threshold': [-0.05],
    'max_high_retrace': [0.05],
    'price_sma_window': [20, 50],
}

tuner = strat_tuner(start_date='2021-01-01', 
                  end_date='2024-01-01', 
                  symbols=['ETH', 'SOL'], 
                  strat_name=SimpleSMAStrategy, 
                  param_ranges=simple_sma_param_ranges,
                  trade_df_timeframe='1day', 
                  indi_df_timeframe='1day') 

trades_df, charts_df, results_df, rolling_results_df = tuner.multi_symbols_param_tuning()

# # save to files
trades_df.to_csv('./test_tuning_trades.csv', index=False)
charts_df.to_csv('./test_tuning_charts.csv', index=False)
results_df.to_csv('./test_tuning_results.csv', index=False)
rolling_results_df.to_csv('./test_tuning_rolling_results.csv', index=False)

# update db with file
db = backtest_performances_db_refresher("backtest_performances")
db.connect_to_db()
db.create_table()
# db.clear_data()
db.insert_data('./test_tuning_rolling_results.csv')
db.close()
 
db = backtest_charts_db_refresher("backtest_charts")
db.connect_to_db() 
db.create_table()
# db.clear_data()
db.insert_data('./test_tuning_charts.csv')
db.close()

db = backtest_trades_db_refresher("backtest_trades")
db.connect_to_db()
db.create_table()
# db.clear_data()
db.insert_data('./test_tuning_trades.csv')
db.close() 