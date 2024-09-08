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

# # avan stock data ingestion
# months = ['2021-08', '2021-09', '2021-10', '2021-11', '2021-12', '2022-01', '2022-02', '2022-03', '2022-04', '2022-05', '2022-06', '2022-07', '2022-08', '2022-09', '2022-10', '2022-11', '2022-12', '2023-01', '2023-02', '2023-03', '2023-04', '2023-05', '2023-06', '2023-07', '2023-08', '2023-09', '2023-10', '2023-11', '2023-12', '2024-01', '2024-02', '2024-03', '2024-04', '2024-05', '2024-06', '2024-07', '2024-08']
 
# Save data to CSV files
# avan_intraday_stock_data_as_csv(interval='60min', 
#                                 months=months,
#                                 ticker='PYPL', 
#                                 avan_api_key=avan_api_key, 
#                                 outputsize='full').to_csv('min_df_chart.csv', index=False)

# avan_daily_stock_data_as_csv(ticker='PYPL', 
#                              avan_api_key=avan_api_key, 
#                              outputsize='full', num_rows=2000).to_csv('day_df_chart.csv', index=False)

# avan_daily_stock_data_as_csv(ticker='GAP', 
#                              avan_api_key=avan_api_key, 
#                              outputsize='full', num_rows=2000).to_csv('extra_day_df_chart.csv', index=False)

# # Read data from CSV files
# min_df_chart = pd.read_csv('./min_df_chart.csv')
# day_df_chart = pd.read_csv('./day_df_chart.csv')
# extra_day_df_chart = pd.read_csv('./extra_day_df_chart.csv')

orders_df = pd.DataFrame()
exec_df = pd.DataFrame()  
# pull monitored symbols
monitored_pairs_query = '''
select distinct a.symbol
from binance_coin_hourly_historical_price a 
join coin_stonewell_signal b 
on a.symbol=b.symbol
where (
    (b.close_above_sma is true and b.death_cross is true) or
    (b.close_above_sma is true and b.rsi_above_sma is true) or
    (b.close_above_sma is true and b.short_vol_above_long is true) or
    (b.death_cross is true and b.rsi_above_sma is true) or
    (b.death_cross is true and b.short_vol_above_long is true) or
    (b.rsi_above_sma is true and b.short_vol_above_long is true)
)
and b.symbol <> 'USDC';
'''

conn = connect_to_db(DB_NAME, DB_HOST, DB_USERNAME, DB_PASSWORD)
monitored_pairs = pd.read_sql(monitored_pairs_query, conn)
# monitored_pairs = pd.DataFrame({'symbol': ['BTC', 'ETH', 'SOL', 'AVAX', 'ADA', 'DOT', 'LINK', 'XRP', 'MATIC']})
monitored_pairs = pd.DataFrame({'symbol': ['BTC']})
print(monitored_pairs)
# Loop through each symbol


all_trade_summaries = {}

for symbol in monitored_pairs['symbol']:
    min_df_query = f'''
    select *
    from binance_coin_hourly_historical_price 
    where symbol = '{symbol}'
    and date > '2024-08-01';
    '''
    day_df_query = f'''
    select *
    from binance_coin_historical_price 
    where symbol = '{symbol}'
    and date > '2024-08-01';
    '''
    
    min_df_chart = pd.read_sql(min_df_query, conn)
    day_df_chart = pd.read_sql(day_df_query, conn)
    ts = StoneWellStrategy_v2(trade_candles_df=min_df_chart,  
                       indicator_candles_df=min_df_chart,  
                       executions_df=pd.DataFrame(),
                       open_orders_df=pd.DataFrame(),
                       tlt_dollar=1000,
                       commission_pct=0.001,
                       extra_indicator_candles_df=day_df_chart,
                       profit_threshold=10,
                       stoploss_threshold=-0.05,
                       max_open_orders_per_symbol=1,
                       max_open_orders_total=3,
                       # fine tuning
                       rsi_window=14,
                       rsi_sma_window=30,
                       price_sma_window=20,
                       short_sma_window=20,
                       long_sma_window=100,
                       volume_short_sma_window=7,
                       volume_long_sma_window=30
                       )
 
    
    # Run test for this symbol
    ts.run_test()
    print(ts.trade_candles_df.columns)
    ts.trade_candles_df.to_csv('./test.csv', index=False)
    ts.indicator_candles_df.to_csv('./test_indi.csv', index=False)
    ts.extra_indicator_candles_df.to_csv('./test_extra_indi.csv', index=False)
    trade_summary = ts.trading_summary()
    all_trade_summaries[symbol] = trade_summary
    
# Print all trading summaries at the end
for symbol, summary in all_trade_summaries.items():
    if summary:  # Only print if summary is not empty
        print(f"\nTrading Summary for {symbol}:")
        for key, value in summary.items():
            print(f"{key}: {value}")
    
# ts.trade_candles_df.to_csv('./test.csv', index=False)
# ts.open_orders_df.to_csv('./test_open_orders.csv', index=False)
# ts.executions_df.to_csv('./test_exec.csv', index=False)
# ts.indicator_candles_df.to_csv('./test_indi.csv', index=False)
# ts.extra_indicator_candles_df.to_csv('./test_extra_indi.csv', index=False)

conn.close()

