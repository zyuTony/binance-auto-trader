from abc import ABC, abstractmethod
import json
import logging
from datetime import datetime
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects
import requests
import time
from binance.client import Client

DATA_FOLDER = '/home/ec2-user/binance_pair_trader/isolated_bn_data_db_updater/data'

# FOLDERS
RAW_CSV_PATH = DATA_FOLDER + '/raw_csv'
CHECKPOINT_JSON_PATH = DATA_FOLDER + '/checkpoints'
COINT_CSV_PATH = DATA_FOLDER + '/rolling_coint_result_csv'
SIGNAL_CSV_PATH = DATA_FOLDER + '/signal_csv'

# BINANCE JSON 
BN_MAX_RETRIES = 3
BN_CHECKPOINT_FILE = CHECKPOINT_JSON_PATH + '/binance_checkpoint.json'
BN_JSON_PATH = DATA_FOLDER + '/binance_raw_json'

# COIN GECKO JSON
DAYS_PER_API_LIMIT = 180
DAYS_PER_API_LIMIT_HOURLY = 31
GECKO_JSON_PATH = DATA_FOLDER + '/gecko_raw_json'
GECKO_DAILY_JSON_PATH = DATA_FOLDER + '/gecko_raw_json/daily'
GECKO_HOURLY_JSON_PATH = DATA_FOLDER + '/gecko_raw_json/hourly'


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)8s | %(message)s',
    datefmt='%Y-%m-%d %H:%M' #datefmt='%Y-%m-%d %H:%M:%S'
)
 
class api_getter(ABC): 
    def __init__(self, api_key, data_save_path):
        self.api_key = api_key
        self.data_save_path = data_save_path
    
    @abstractmethod
    def _get_download_symbol_list(self):
        pass
    
    @abstractmethod
    def download_data(self):
        pass

"""COIN GECKO"""    
class coin_gecko_daily_ohlc_api_getter(api_getter):
    
    def __init__(self, api_key, data_save_path, start_date, end_date):
        super().__init__(api_key, data_save_path)
        self.num_download_symbols = 300 
        self.start_date = start_date if start_date is not None else datetime.strptime('2018-02-10', '%Y-%m-%d')
        self.end_date = end_date if end_date is not None else datetime.now() 
        self.overview_save_path = GECKO_JSON_PATH+'/mapping/top_symbol_by_mc.json'
        
    def _get_unix_from_date_object(self, date_object):
        return int(date_object.timestamp())
       
    def _pull_coin_list_ranking(self, page_num):
        url = f"https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&order=market_cap_desc&per_page=250&page={page_num}"
        headers = {
            "accept": "application/json",
            "x-cg-pro-api-key": self.api_key
        }
        max_retries = 3
        retry_delay = 5  # seconds

        for attempt in range(max_retries):
            try:
                response = requests.get(url, headers=headers)
                response.raise_for_status()  # Raise an exception for bad status codes
                data = response.json()

                with open(self.overview_save_path, 'w') as file:
                    json.dump(data, file, indent=4)
                
                return data
            except (ConnectionError, Timeout, TooManyRedirects, requests.exceptions.RequestException) as e:
                if attempt < max_retries - 1:
                    logging.warning(f"Attempt {attempt + 1} failed: {e}. Retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                else:
                    logging.error(f"All {max_retries} attempts failed: {e}")
                    return None
 
    def _get_download_symbol_list(self):
        # get top coins
        symbols_ranking = []
        for page_num in range(1, 4): # pull top 1500 coins just in case
            symbols_ranking.extend(self._pull_coin_list_ranking(page_num))

        ids = [item["id"] for item in symbols_ranking][:self.num_download_symbols]
        symbols = [item["symbol"].upper() for item in symbols_ranking][:self.num_download_symbols]
        return ids, symbols
    
    def _download_single_symbol(self, id, symbol):
        unix_start = self._get_unix_from_date_object(self.start_date)
        unix_end = self._get_unix_from_date_object(self.end_date)
        all_data = []    
        current_start = unix_start
        while current_start < unix_end:
            current_end = min(current_start + DAYS_PER_API_LIMIT * 24 * 60 * 60, unix_end)
            try:
                url = f"https://pro-api.coingecko.com/api/v3/coins/{id}/ohlc/range?vs_currency=usd&from={current_start}&to={current_end}&interval=daily"
                headers = {
                    "accept": "application/json",
                    "x-cg-pro-api-key": self.api_key
                }            
                response = requests.get(url, headers=headers)
                if response.status_code == 200:
                    all_data.extend(response.json())  
                    logging.debug(f"Downloaded data for {symbol} from {current_start} to {current_end}")
                else:
                    logging.error(f"Failed to download data for {symbol} from {current_start} to {current_end}: {response.status_code} {response.text}")
                
                current_start = current_end + 1
            except Exception as e:
                logging.exception(f"Exception occurred while downloading data for {symbol} from {unix_start} to {current_end}: {e}")
                break
        return all_data

    def download_data(self):
        ids, symbols = self._get_download_symbol_list()
        
        logging.debug(f"start downloading symbol list:{symbols}") 
        
        for id, symbol in zip(ids, symbols):
            all_data = self._download_single_symbol(id, symbol)
            with open(self.data_save_path+f'/{symbol}.json', 'w') as file:
                json.dump(all_data, file, indent=4)
            logging.info(f"Saved full data for {symbol} to {symbol}.json")
         
class coin_gecko_hourly_ohlc_api_getter(coin_gecko_daily_ohlc_api_getter):     
    '''slight change of download_data from daily ohlc api'''
    def _download_single_symbol(self, id, symbol):
        unix_start = self._get_unix_from_date_object(self.start_date)
        unix_end = self._get_unix_from_date_object(self.end_date)
        all_data = []    
        current_start = unix_start
        while current_start < unix_end:
            current_end = min(current_start + DAYS_PER_API_LIMIT_HOURLY * 24 * 60 * 60, unix_end)
            try:
                url = f"https://pro-api.coingecko.com/api/v3/coins/{id}/ohlc/range?vs_currency=usd&from={current_start}&to={current_end}&interval=hourly"
                headers = {
                    "accept": "application/json",
                    "x-cg-pro-api-key": self.api_key
                }            
                response = requests.get(url, headers=headers)
                if response.status_code == 200:
                    all_data.extend(response.json())  
                    logging.debug(f"Downloaded data for {symbol} from {current_start} to {current_end}")
                else:
                    logging.error(f"Failed to download data for {symbol} from {current_start} to {current_end}: {response.status_code} {response.text}")
                
                current_start = current_end + 1
            except Exception as e:
                logging.exception(f"Exception occurred while downloading data for {symbol} from {unix_start} to {current_end}: {e}")
                break
        return all_data

'''BINANCE'''
class binance_ohlc_api_getter(coin_gecko_daily_ohlc_api_getter):
    '''Binance api data download that include volume data'''
    def __init__(self, api_key, api_secret, data_save_path, interval, start_date, end_date):
        super().__init__(api_key, data_save_path, None, None)
        self.num_download_symbols = 300 
        self.api_secret = api_secret
        self.client = Client(self.api_key, self.api_secret)
        self.interval = interval
        self.start_date = start_date
        self.end_date = end_date
        
    def _download_single_symbol(self, symbol):
        try:
            # Get data and save to JSON
            ticker_data = self.client.get_historical_klines(symbol+'USDT', self.interval, self.start_date, self.end_date)
            with open(f'{self.data_save_path}/{self.interval.split("_")[-1]}/{symbol}.json', 'w') as file:
                json.dump(ticker_data, file, indent=4)
            logging.info(f'Downloaded {symbol}')
            return 1
        except Exception as e:
            logging.error(f"Error downloading {symbol}: {e}")
            return -1

    def download_data(self): 
        _, symbols = self._get_download_symbol_list()
        for symbol in symbols:
            self._download_single_symbol(symbol)
            
        
        
 