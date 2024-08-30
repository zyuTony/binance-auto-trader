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

 
client = Client(api_key, api_secret) 
# get asset pair bought amt
balance = float(client.get_asset_balance(
asset='USDT')['free'])
print(balance)
