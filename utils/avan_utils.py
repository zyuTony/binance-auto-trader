import requests
import os
import json
import time 
import pandas as pd

def avan_single_json_append_to_csv(json_file):
    with open(json_file, 'r') as file:
        data = json.load(file)
        symbol = data["Meta Data"]["2. Symbol"]
        daily_data = data["Time Series (Daily)"]
        
        # Create a dataframe with Date and Close price
        df = pd.DataFrame({
            'Date': list(daily_data.keys()),
            symbol: [daily_data[date]["4. close"] for date in daily_data]
        })
        df['Date'] = pd.to_datetime(df['Date'])
        return df

def avan_pull_stock_data_as_csv(interval, ticker, avan_api_key, outputsize):
    url = f'https://www.alphavantage.co/query?function=TIME_SERIES_{interval}_ADJUSTED&symbol={ticker}&outputsize={outputsize}&datatype=csv&apikey={avan_api_key}'

    response = requests.get(url)
    json_file_path = AVAN_JSON_PATH + f'/avan_data_{interval}/{ticker}_{interval}.json'
    if response.status_code == 200:
        data = response.json()
        with open(json_file_path, 'w') as file:
            json.dump(data, file, indent=4)
        # print(f"{ticker} saved to json")
        return 1 
    else:
        print(f"Error fetching {ticker}: {response.status_code}")
        return -1
       
def avan_pull_stock_data(interval, ticker, avan_api_key, outputsize):
    url = f'https://www.alphavantage.co/query?function=TIME_SERIES_{interval}_ADJUSTED&symbol={ticker}&outputsize={outputsize}&apikey={avan_api_key}'

    response = requests.get(url)
    json_file_path = AVAN_JSON_PATH + f'/avan_data_{interval}/{ticker}_{interval}.json'
    if response.status_code == 200:
        data = response.json()
        with open(json_file_path, 'w') as file:
            json.dump(data, file, indent=4)
        # print(f"{ticker} saved to json")
        return 1 
    else:
        print(f"Error fetching {ticker}: {response.status_code}")
        return -1

def avan_pull_stock_overview(ticker, avan_api_key):
    url = f'https://www.alphavantage.co/query?function=OVERVIEW&symbol={ticker}&apikey={avan_api_key}'

    response = requests.get(url)
    json_file_path = AVAN_OVERVIEW_JSON_PATH + f'/{ticker}.json'
    if response.status_code == 200:
        data = response.json()
        with open(json_file_path, 'w') as file:
            json.dump(data, file, indent=4)
        # print(f"{ticker} saved to json")
        return 1 
    else:
        print(f"Error fetching {ticker}: {response.status_code}")
        return -1

def avan_pull_option(ticker, avan_api_key, option_type='HISTORICAL'):
    url = f'https://www.alphavantage.co/query?function=HISTORICAL_OPTIONS&symbol={ticker}&apikey={avan_api_key}'

    response = requests.get(url)
    json_file_path = AVAN_OVERVIEW_JSON_PATH + f'/{ticker}.json'
    if response.status_code == 200:
        data = response.json()
        with open(json_file_path, 'w') as file:
            json.dump(data, file, indent=4)
        # print(f"{ticker} saved to json")
        return 1 
    else:
        print(f"Error fetching {ticker}: {response.status_code}")
        return -1
    
def avan_pull_stocks_hist_price_to_json(avan_api_key, interval, tickers, data_length='full'):
    if os.path.exists(AVAN_CHECKPOINT_FILE):
        with open(AVAN_CHECKPOINT_FILE, 'r') as file:
            checkpoint_data = json.load(file)
    else:
        checkpoint_data = []
    for ticker in tickers:
        if ticker in checkpoint_data:
            print(f"Skipping {ticker}, already downloaded.")
            continue
        status = avan_pull_stock_data(interval, ticker, avan_api_key, outputsize=data_length)
        time.sleep(AVAN_SLEEP_TIME)
        if status == 1:
            checkpoint_data.append(ticker)
            with open(AVAN_CHECKPOINT_FILE, 'w') as file:
                json.dump(checkpoint_data, file, indent=4)


def avan_pull_stocks_overview_json(avan_api_key, tickers):
    if os.path.exists(AVAN_OVERVIEW_CHECKPOINT_FILE):
        with open(AVAN_OVERVIEW_CHECKPOINT_FILE, 'r') as file:
            checkpoint_data = json.load(file)
    else:
        checkpoint_data = []
    for ticker in tickers:
        if ticker in checkpoint_data:
            print(f"Skipping {ticker} overview, already downloaded.")
            continue
        status = avan_pull_stock_overview(ticker, avan_api_key)
        time.sleep(AVAN_SLEEP_TIME)
        if status == 1:
            checkpoint_data.append(ticker)
            with open(AVAN_OVERVIEW_CHECKPOINT_FILE, 'w') as file:
                json.dump(checkpoint_data, file, indent=4)
    print('Download completed! :)')  
