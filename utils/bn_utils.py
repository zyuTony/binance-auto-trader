import json
import pandas as pd
from requests.exceptions import ConnectionError 
from statsmodels.tsa.stattools import coint
import statsmodels.api as sm
import os
import time
from requests.exceptions import ReadTimeout, ConnectionError
from tqdm import tqdm 
from datetime import datetime
from binance.client import Client
from utils.cmc_utils import *
from config import *


def bn_pull_input_coins_hist_price_json(symbols, bn_api_key, bn_api_secret, start_date, end_date, interval, interval_name):
  coin_ids = [symbol+'USDT' for symbol in symbols]
  print(coin_ids)

  # --- Binance
  client = Client(bn_api_key, bn_api_secret)
  # pull data to json
  if os.path.exists(CHECKPOINT_JSON_PATH+'/alt_analysis_data.json'):
      with open(CHECKPOINT_JSON_PATH+'/alt_analysis_data.json', 'r') as file:
          checkpoint_data = json.load(file)
  else:
      checkpoint_data = []

  for coin_id in coin_ids:
      if coin_id in checkpoint_data:
          print(f"Skipping {coin_id}, already downloaded.")
          continue
      status = get_ticker_by_interval_name(client, coin_id, interval, interval_name, start_date, end_date, DATA_FOLDER+'/alt_analysis_data')
      if status == 1:
          checkpoint_data.append(coin_id)
          with open(CHECKPOINT_JSON_PATH+'/alt_analysis_data.json', 'w') as file:
              json.dump(checkpoint_data, file, indent=4)
  print('Download completed! :)')

def bn_pull_top_coins_hist_price_json(cmc_api_key, bn_api_key, bn_api_secret, start_date, end_date, interval, interval_name, top_n_coins=300):
  # --- CMC
  data = pull_coin_list(top_n_coins, cmc_api_key)
  _, symbols= coin_list_json_to_array(data)
  coin_ids = [symbol+'USDT' for symbol in symbols]
  print(coin_ids)

  # --- Binance
  client = Client(bn_api_key, bn_api_secret)
  # pull data to json
  if os.path.exists(config.BN_CHECKPOINT_FILE):
      with open(config.BN_CHECKPOINT_FILE, 'r') as file:
          checkpoint_data = json.load(file)
  else:
      checkpoint_data = []

  for coin_id in coin_ids:
      if coin_id in checkpoint_data:
          print(f"Skipping {coin_id}, already downloaded.")
          continue
      status = get_ticker_by_interval_name(client, coin_id, interval, interval_name, start_date, end_date, config.BN_JSON_PATH)
      if status == 1:
          checkpoint_data.append(coin_id)
          with open(config.BN_CHECKPOINT_FILE, 'w') as file:
              json.dump(checkpoint_data, file, indent=4)
  print('Download completed! :)')

def get_ticker_by_interval_name(client, coin_id, interval, interval_name, start_date, end_date, download_folder):
    retry_count = 0
    while retry_count < config.BN_MAX_RETRIES:
        try:
            # Get data and save to JSON
            ticker_data = client.get_historical_klines(coin_id, interval, start_date, end_date)
            with open(download_folder + f'/{interval_name}/{coin_id}.json', 'w') as file:
                json.dump(ticker_data, file, indent=4)
                print(f'Downloaded {coin_id}')
            return 1  
        
        except (ReadTimeout, ConnectionError) as e:
            retry_count += 1
            print(f"Network Error {coin_id}: {e}")
            print(f"Retry in {2 ** retry_count} seconds...")
            time.sleep(2 ** retry_count) # Exponential backoff
        
        except Exception as e:
            print(f"Error For {coin_id}: {e}")
            break  # Break the loop on other exceptions
    return -1

def agg_data_to_csv(output_file, coin_list=None, num_of_coins=200):
    if coin_list is None:
        with open("home/ec2-user/financial_database/backend/checkpoint.json", 'r') as file:
            checkpoint_data = json.load(file)
        coin_ids = checkpoint_data['30MINUTES'][:num_of_coins]
    else:
        coin_ids = coin_list

    # extract data in csv with columns: Date, BTCUSD, ETHUSD, ...
    df_list = []
    for coin_id in tqdm(coin_ids, desc="Processing coins"):
        with open(f'/TODO/{coin_id}.json', 'r') as file:
            data = json.load(file)
        temp_data = []
        for entry in data:
            open_time = pd.to_datetime(entry[0], unit='ms', utc=True).strftime('%Y-%m-%d %H:%M:%S')
            close_price = entry[4]
            temp_data.append([open_time, close_price])

        temp_df = pd.DataFrame(temp_data, columns=['date', coin_id])
        temp_df['date'] = pd.to_datetime(temp_df['date'])
        df_list.append(temp_df)
        print(f'\nCompleted {coin_id}')

    df = pd.concat(df_list, axis=1)
    df = df.loc[:, ~df.columns.duplicated()]
    df = df.dropna(axis=1, how='all')
    df.to_csv(output_file, index=False)
    print("Data has been written to coin_prices.csv")
    return df
 
 

# KLINE_INTERVAL_1MINUTE 
# KLINE_INTERVAL_3MINUTE 
# KLINE_INTERVAL_5MINUTE 
# KLINE_INTERVAL_15MINUTE 
# KLINE_INTERVAL_30MINUTE 
# KLINE_INTERVAL_1HOUR 
# KLINE_INTERVAL_2HOUR 
# KLINE_INTERVAL_4HOUR 
# KLINE_INTERVAL_6HOUR 
# KLINE_INTERVAL_8HOUR 
# KLINE_INTERVAL_12HOUR 
# KLINE_INTERVAL_1DAY 
# KLINE_INTERVAL_3DAY 
# KLINE_INTERVAL_1WEEK 
# KLINE_INTERVAL_1MONTH 

# extract all price data
# with open(f'./{coin_id}.json', 'r') as file:
#     data = json.load(file)
# extracted_data = []
# for entry in data:
#     open_time = datetime.utcfromtimestamp(entry[0] / 1000).strftime('%Y-%m-%d %H:%M:%S')
#     open_price = entry[1]
#     high_price = entry[2]
#     low_price = entry[3]
#     close_price = entry[4]
#     volume = entry[5]
#     extracted_data.append([open_time, open_price, high_price, low_price, close_price, volume])

def get_all_ticker_by_intervals(client, intervals, start_date, end_date):
    
    data = client.get_all_tickers()
    coin_ids = [item["symbol"] for item in data if item["symbol"].endswith("USDT")]

    checkpoint_file = '.home/ec2-user/financial_database/backend/binance_data_2/checkpoint.json'
    if os.path.exists(checkpoint_file):
        with open(checkpoint_file, 'r') as file:
            checkpoint_data = json.load(file)
    else:
        checkpoint_data = {interval: [] for interval in intervals}

    for interval in intervals:
        if interval not in checkpoint_data:
            checkpoint_data[interval] = []
        print(f"retrieve data for {interval}")

        for coin_id in coin_ids:
            if coin_id in checkpoint_data[interval]:
                print(f"Skipping {coin_id}, already downloaded.")
                continue
            
            retry_count = 0
            while retry_count < config.BN_MAX_RETRIES:
                try:
                    # Get data and save to JSON
                    ticker_data = client.get_historical_klines(coin_id, Client.KLINE_INTERVAL_30MINUTE, start_date, end_date)
                    with open(f'/TODO/{coin_id}.json', 'w') as file:
                        json.dump(ticker_data, file, indent=4)
                        print(f'Downloaded {coin_id}')
                    
                    # Update checkpoint data
                    checkpoint_data[interval].append(coin_id)
                    with open(checkpoint_file, 'w') as file:
                        json.dump(checkpoint_data, file, indent=4)
                    
                    break  # Break the retry loop on successful download

                except (ReadTimeout, ConnectionError) as e:
                    retry_count += 1
                    wait_time = 2 ** retry_count  # Exponential backoff
                    print(f"Error occurred while downloading {coin_id}: {e}")
                    print(f"Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                
                except Exception as e:
                    print(f"An unexpected error occurred: {e}")
                    break  # Break the loop on other exceptions

    print("Data downloading process completed.")
