'''Connect to websocket for real time data
Cadence: AUTOMATIC DAILY
  1. connect get latest real time data.
     -- get 10 minute bar for Y X
     -- get daily bar
  2. calculate spread.
     -- calculate spread using minute data, coeff and constant.
     -- calculate bb bands with 20 and 1.8 with daily data every 10 minutes 
  3. trade based on the spread. long x and short y.
     -- long y short x when reach lower band
     -- long x short y when reach upper band
  4. close trade if reach profit or stop loss.
     -- 
     -- close the long and short when reach rolling mean on spread
     -- close long and short when lost reach 10% on both.
'''  

import os
from dotenv import load_dotenv  
import pandas as pd
from datetime import datetime, timezone
from binance.client import Client 
from utils.trading_utils import *

from binance.helpers import round_step_size

load_dotenv()
DB_USERNAME = os.getenv('RDS_USERNAME') 
DB_PASSWORD = os.getenv('RDS_PASSWORD') 
DB_HOST = os.getenv('RDS_ENDPOINT') 
DB_NAME = 'financial_data'

api_key = os.getenv('BINANCE_API')  
api_secret = os.getenv('BINANCE_SECRET')  
client = Client(api_key, api_secret)

 