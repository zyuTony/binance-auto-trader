import os
from dotenv import load_dotenv
import pandas as pd
from binance.client import Client
from binance.helpers import round_step_size
from binance.enums import *
from utils.strat_utils import *
from utils.trading_utils import *
import sys

load_dotenv()
api_key = os.getenv('BINANCE_API')
api_secret = os.getenv('BINANCE_SECRET')
DB_USERNAME = os.getenv('RDS_USERNAME')
DB_PASSWORD = os.getenv('RDS_PASSWORD')
DB_HOST = os.getenv('RDS_ENDPOINT')
DB_NAME = 'financial_data'


'''testing pipeline'''
# client = Client(api_key, api_secret)
# min_df_chart, day_df_chart = get_bn_data(client, 'BTCUSDT')

# orders_df = pd.DataFrame()
# exec_df = pd.DataFrame()  

# ts = DummyStrategy(min_df_chart, min_df_chart, exec_df, orders_df)
# ts.run_test()
 
# min_df_chart.to_csv('./test.csv', index=False)
# ts.open_orders_df.to_csv('./test_open_orders.csv', index=False)
# ts.executions_df.to_csv('./test_exec.csv', index=False)
# ts.indicator_candles_df.to_csv('./test_indi.csv', index=False)


'''production pipeline'''
order_csv_file = './order_df.csv'
exec_csv_file = './exec_df.csv'
ideal_exec_csv_file = './ideal_exec_df.csv'

if os.path.exists(order_csv_file):
    orders_df = pd.read_csv(order_csv_file)
    if orders_df.empty:
        orders_df = pd.DataFrame()
else:
    orders_df = pd.DataFrame()
    
if os.path.exists(exec_csv_file):
    exec_df = pd.read_csv(exec_csv_file)
    if exec_df.empty:
        exec_df = pd.DataFrame()
else:
    exec_df = pd.DataFrame()     
    
if os.path.exists(ideal_exec_csv_file):
    ideal_exec_df = pd.read_csv(ideal_exec_csv_file)
    if ideal_exec_df.empty:
        ideal_exec_df = pd.DataFrame()
else:
    ideal_exec_df = pd.DataFrame()     
    
client = Client(api_key, api_secret)
min_df_chart, day_df_chart = get_bn_data(client, 'BTCUSDT')
ts = BNDummyStrategy(client, min_df_chart, day_df_chart, exec_df, ideal_exec_df, orders_df, tlt_dollar=20)

ts.run_once()

min_df_chart.to_csv('./test.csv', index=False)
ts.open_orders_df.to_csv(order_csv_file, index=False)
ts.executions_df.to_csv(exec_csv_file, index=False)
ts.ideal_executions_df.to_csv(ideal_exec_csv_file, index=False)


