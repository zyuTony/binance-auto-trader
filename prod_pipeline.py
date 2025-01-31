import os
from dotenv import load_dotenv
import pandas as pd
from binance.client import Client
from binance.helpers import round_step_size
from binance.enums import *
from utils.strat_utils import *
from utils.trading_utils import *
from utils.avan_utils import *
import sys

load_dotenv()
api_key = os.getenv('BINANCE_API')
api_secret = os.getenv('BINANCE_SECRET')
avan_api_key = os.getenv('ALPHA_VANTAGE_PREM_API') 

DB_USERNAME = os.getenv('RDS_USERNAME')
DB_PASSWORD = os.getenv('RDS_PASSWORD')
DB_HOST = os.getenv('RDS_ENDPOINT')
 DB_NAME = os.getenv('RDS_DB_NAME')


'''production pipeline'''
# TODO change for each stratgegy
DATA_FOLDER = './data/'
STRAT_NAME = 'stonewell'
order_csv_file = DATA_FOLDER + f'{STRAT_NAME}_order_df.csv'
exec_csv_file = DATA_FOLDER + f'{STRAT_NAME}_exec_df.csv'
ideal_exec_csv_file = DATA_FOLDER + f'{STRAT_NAME}_ideal_exec_df.csv'

orders_df = pd.read_csv(order_csv_file) if os.path.exists(order_csv_file) else pd.DataFrame()
exec_df = pd.read_csv(exec_csv_file) if os.path.exists(exec_csv_file) else pd.DataFrame()
ideal_exec_df = pd.read_csv(ideal_exec_csv_file) if os.path.exists(ideal_exec_csv_file) else pd.DataFrame()
  
client = Client(api_key, api_secret)
min_df_chart, day_df_chart = get_bn_data(client, 'BTCUSDT')

# TODO CHANGE WITH NEW STRATEGY
ts = StoneWellStrategy(trade_candles_df=min_df_chart,  
                       indicator_candles_df=day_df_chart,  
                       executions_df=pd.DataFrame(),
                       open_orders_df=pd.DataFrame(),
                       tlt_dollar=20,
                       commission_pct=None,
                       extra_indicator_candles_df=None,
                       ideal_executions_df=ideal_exec_df,
                       # fine tuning
                       profit_threshold=10,
                       stoploss_threshold=-0.05,
                       max_open_orders_per_symbol=1,
                       max_open_orders_total=3, 
                       rsi_window=14,
                       rsi_sma_window=10,
                       price_sma_window=20,
                       short_sma_window=50,
                       long_sma_window=100,
                       volume_short_sma_window=7,
                       volume_long_sma_window=30)
ts.run_once()

min_df_chart.to_csv('./test.csv', index=False)
ts.open_orders_df.to_csv(order_csv_file, index=False)
ts.executions_df.to_csv(exec_csv_file, index=False)
ts.ideal_executions_df.to_csv(ideal_exec_csv_file, index=False)


