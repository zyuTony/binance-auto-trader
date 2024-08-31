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
months = ['2023-08', '2023-09', '2023-10', '2023-11', '2023-12',
          '2024-01', '2024-02', '2024-03', '2024-04', '2024-05',
          '2024-06', '2024-07', '2024-08']
min_df_chart = avan_intraday_stock_data_as_csv(interval='30min', 
                                          months=months,
                                          ticker='DKS', 
                                          avan_api_key=avan_api_key, 
                                          outputsize='full')
 
print(min_df_chart)
 