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
DB_NAME = 'financial_data'

''' date|open|high|low|close|volume'''
'''testing pipeline'''
# binance data
# client = Client(api_key, api_secret)
# min_df_chart, day_df_chart = get_bn_data(client, 'BTCUSDT')

# avan stock data ingestion
# months = ['2023-08', '2023-09', '2023-10', '2023-11', '2023-12',
#           '2024-01', '2024-02', '2024-03', '2024-04', '2024-05',
#           '2024-06', '2024-07', '2024-08']
# # Save data to CSV files
# avan_intraday_stock_data_as_csv(interval='30min', 
#                                 months=months,
#                                 ticker='DKS', 
#                                 avan_api_key=avan_api_key, 
#                                 outputsize='full').to_csv('min_df_chart.csv', index=False)

avan_daily_stock_data_as_csv(ticker='DKS', 
                             avan_api_key=avan_api_key, 
                             outputsize='full', num_rows=720).to_csv('day_df_chart.csv', index=False)

avan_daily_stock_data_as_csv(ticker='GAP', 
                             avan_api_key=avan_api_key, 
                             outputsize='full', num_rows=720).to_csv('extra_day_df_chart.csv', index=False)

# Read data from CSV files
min_df_chart = pd.read_csv('./min_df_chart.csv')
day_df_chart = pd.read_csv('./day_df_chart.csv')
extra_day_df_chart = pd.read_csv('./extra_day_df_chart.csv')

orders_df = pd.DataFrame()
exec_df = pd.DataFrame()  

ts = BuyTheDipStrategy(trade_candles_df=min_df_chart, 
                        indicator_candles_df=day_df_chart, 
                        executions_df=exec_df, 
                        open_orders_df=orders_df,
                        tlt_dollar=100,
                        commission_pct=0.001,
                        extra_indicator_candles_df=extra_day_df_chart) # for test only
ts.run_test()
ts.trading_summary()

ts.trade_candles_df.to_csv('./test.csv', index=False)
ts.open_orders_df.to_csv('./test_open_orders.csv', index=False)
ts.executions_df.to_csv('./test_exec.csv', index=False)
ts.indicator_candles_df.to_csv('./test_indi.csv', index=False)
ts.extra_indicator_candles_df.to_csv('./test_extra_indi.csv', index=False)



'''production pipeline'''
# order_csv_file = './order_df.csv'
# exec_csv_file = './exec_df.csv'
# ideal_exec_csv_file = './ideal_exec_df.csv'

# if os.path.exists(order_csv_file):
#     orders_df = pd.read_csv(order_csv_file)
#     if orders_df.empty:
#         orders_df = pd.DataFrame()
# else:
#     orders_df = pd.DataFrame()
    
# if os.path.exists(exec_csv_file):
#     exec_df = pd.read_csv(exec_csv_file)
#     if exec_df.empty:
#         exec_df = pd.DataFrame()
# else:
#     exec_df = pd.DataFrame()     
    
# if os.path.exists(ideal_exec_csv_file):
#     ideal_exec_df = pd.read_csv(ideal_exec_csv_file)
#     if ideal_exec_df.empty:
#         ideal_exec_df = pd.DataFrame()
# else:
#     ideal_exec_df = pd.DataFrame()     

    
# client = Client(api_key, api_secret)
# min_df_chart, day_df_chart = get_bn_data(client, 'BTCUSDT')
# ts = BNDummyStrategy(client, min_df_chart, day_df_chart, exec_df, ideal_exec_df, orders_df, tlt_dollar=20)

# ts.run_once()

# min_df_chart.to_csv('./test.csv', index=False)
# ts.open_orders_df.to_csv(order_csv_file, index=False)
# ts.executions_df.to_csv(exec_csv_file, index=False)
# ts.ideal_executions_df.to_csv(ideal_exec_csv_file, index=False)


