from dotenv import load_dotenv
import os
from api_utils import *
from db_utils import *
from binance.client import Client

load_dotenv(override=True)
bn_api_key = os.getenv('BINANCE_API')  
bn_api_secret = os.getenv('BINANCE_SECRET')  
cmc_api_key = os.getenv('CMC_API')  
avan_api_key = os.getenv('ALPHA_VANTAGE_PREM_API') 
gc_api_key = os.getenv('GECKO_API') 

DB_USERNAME = os.getenv('RDS_USERNAME') 
DB_PASSWORD = os.getenv('RDS_PASSWORD') 
DB_HOST = os.getenv('RDS_ENDPOINT') 
 DB_NAME = os.getenv('RDS_DB_NAME')

# '''DAILY DATA REFRESH'''
# bn_data = binance_ohlc_api_getter(api_key=bn_api_key,
#                              api_secret=bn_api_secret,
#                              data_save_path=BN_JSON_PATH,
#                              interval=Client.KLINE_INTERVAL_1DAY,
#                              start_date='1 Jan, 2018',
#                              end_date='3 Sep, 2024')
# bn_data.download_data()

# db = binance_OHLC_db_refresher("binance_coin_historical_price")
# db.connect_to_db()
# db.create_table()
# for filename in os.listdir(BN_JSON_PATH+'/1d'):
#     if filename.endswith('.json'):
#         file_path = os.path.join(BN_JSON_PATH+'/1d', filename)
#         db.insert_data(file_path)
# db.close()

# '''HOURLY DATA REFRESH'''
# bn_data = binance_ohlc_api_getter(api_key=bn_api_key,
#                              api_secret=bn_api_secret,
#                              data_save_path=BN_JSON_PATH,
#                              interval=Client.KLINE_INTERVAL_1HOUR,
#                              start_date='1 Jan, 2018',
#                              end_date='3 Sep, 2024')
# bn_data.download_data()

# db = binance_OHLC_db_refresher("binance_coin_hourly_historical_price")
# db.connect_to_db()
# db.create_table()
# for filename in os.listdir(BN_JSON_PATH+'/1h'):
#     if filename.endswith('.json'):
#         file_path = os.path.join(BN_JSON_PATH+'/1h', filename)
#         db.insert_data(file_path)
# db.close()

# '''4 HOURS DATA REFRESH'''
# bn_data = binance_ohlc_api_getter(api_key=bn_api_key,
#                              api_secret=bn_api_secret,
#                              data_save_path=BN_JSON_PATH,
#                              interval=Client.KLINE_INTERVAL_4HOUR,
#                              start_date='1 Jan, 2018',
#                              end_date='3 Sep, 2024')
# bn_data.download_data()

# db = binance_OHLC_db_refresher("binance_coin_4hours_historical_price")
# db.connect_to_db()
# db.create_table()
# for filename in os.listdir(BN_JSON_PATH+'/4h'):
#     if filename.endswith('.json'):
#         file_path = os.path.join(BN_JSON_PATH+'/4h', filename)
#         db.insert_data(file_path)
# db.close()

'''5 MINUTE DATA REFRESH'''
bn_data = binance_ohlc_api_getter(api_key=bn_api_key,
                             api_secret=bn_api_secret,
                             data_save_path=BN_JSON_PATH,
                             interval=Client.KLINE_INTERVAL_5MINUTE,
                             start_date='1 Jan, 2020',
                             end_date='8 Sep, 2024')
bn_data.num_download_symbols = 20
bn_data.download_data()
 
db = binance_OHLC_db_refresher("binance_coin_5mins_historical_price")
db.connect_to_db()
db.create_table()
for filename in os.listdir(BN_JSON_PATH+'/5m'):
    if filename.endswith('.json'):
        file_path = os.path.join(BN_JSON_PATH+'/5m', filename)
        db.insert_data(file_path)
db.close()