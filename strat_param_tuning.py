import os
from dotenv import load_dotenv
import pandas as pd
from binance.enums import *
from utils.strat_utils import *
from utils.trading_utils import *
from utils.avan_utils import *
import itertools
from datetime import datetime

load_dotenv()
api_key = os.getenv('BINANCE_API')
api_secret = os.getenv('BINANCE_SECRET')
avan_api_key = os.getenv('ALPHA_VANTAGE_PREM_API') 

DB_USERNAME = os.getenv('RDS_USERNAME')
DB_PASSWORD = os.getenv('RDS_PASSWORD')
DB_HOST = os.getenv('RDS_ENDPOINT')
DB_NAME = 'financial_data'

# Read data from CSV files
def parameter_tuning(trade_df, indi_df, strategy_class, param_ranges, extra_indi_df=None):
    results = []
    n = 1
    
    for params in itertools.product(*param_ranges.values()):
        param_dict = dict(zip(param_ranges.keys(), params))
        logging.info(f'Begin trial {n}')
        ts = strategy_class(
            trade_candles_df=trade_df, 
            indicator_candles_df=indi_df, 
            executions_df=pd.DataFrame(), 
            open_orders_df=pd.DataFrame(),
            tlt_dollar=1000,
            commission_pct=0.001,
            extra_indicator_candles_df=extra_indi_df,
            max_open_orders_per_symbol=1,
            max_open_orders_total=3,
            **param_dict
        )
        ts.run_test()
        trade_summary = ts.trading_summary() 
        
        result = {
            'avg_trade_duration': trade_summary['Average Trade Duration'],
            'median_trade_duration': trade_summary['Median Trade Duration'],
            'total_trades': trade_summary['Total Number of Trades'],
            'trades_win_rate': trade_summary['Trades Win Rate'],
            'money_win_loss_ratio': trade_summary['Money Win/Loss Ratio'],
            'profit_factor': trade_summary['key_metric_profit_pct'],
            'buy_hold_profit': trade_summary['Buy and Hold Percent'],
            **param_dict
        }
        results.append(result)
        n += 1

    # Save all results to a CSV file
    results_df = pd.DataFrame(results)
    return results_df

TUNING_FOLDER ='./data/tuning_results/'
param_ranges = {
    'profit_threshold': [10],
    'stoploss_threshold': [-0.05],
    'rsi_window': [7, 14],
    'rsi_sma_window': [20, 50, 100],
    'price_sma_window': [20, 50],
    'short_sma_window': [50],
    'long_sma_window': [200],
    'volume_short_sma_window': [14],
    'volume_long_sma_window': [30], 
}

symbol='BTC'
strat_name='stonewell'
min_df_query = f'''
    select *
    from binance_coin_hourly_historical_price 
    where symbol = '{symbol}'
    and date between '2021-11-01' and '2024-01-01';
    '''
    
day_df_query = f'''
    select *
    from binance_coin_historical_price 
    where symbol = '{symbol}'
    and date between '2021-11-01' and '2024-01-01';
    '''
conn = connect_to_db(DB_NAME, DB_HOST, DB_USERNAME, DB_PASSWORD)
    
min_df_chart = pd.read_sql(min_df_query, conn)
day_df_chart = pd.read_sql(day_df_query, conn)

results = parameter_tuning(min_df_chart, day_df_chart, 
                           StoneWellStrategy, param_ranges,
                           day_df_chart)

results.to_csv(TUNING_FOLDER+f'{symbol}_{strat_name}_tuning_results_{datetime.now().strftime("%Y%m%d_%H%M")}.csv', index=False)



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
