import os
from dotenv import load_dotenv
import pandas as pd
from binance.enums import *
from utils.strat_utils import *
from utils.trading_utils import *
from utils.avan_utils import *
import itertools
from datetime import datetime, timedelta

load_dotenv()
api_key = os.getenv('BINANCE_API')
api_secret = os.getenv('BINANCE_SECRET')
avan_api_key = os.getenv('ALPHA_VANTAGE_PREM_API') 

DB_USERNAME = os.getenv('RDS_USERNAME')
DB_PASSWORD = os.getenv('RDS_PASSWORD')
DB_HOST = os.getenv('RDS_ENDPOINT')
DB_NAME = 'financial_data'


class strat_tuner():
    def __init__(self, start_date, end_date, symbols, strat_names, param_ranges):
        
        self.start_date = datetime.strptime(start_date, '%Y-%m-%d')
        self.end_date = datetime.strptime(end_date, '%Y-%m-%d')
        self.symbols = symbols
        self.strat_names = strat_names
        self.param_ranges = param_ranges
        
        self.train_test_split_date = None
        self.train_data = None
        self.test_data = None
        self.train_test_split_pct = 0.7  # Default value, can be adjusted
        self.rolling_window = 90  # Default 30-day rolling window
        self.rolling_step = 1  # Default 7-day step
    '''
    Goal: find the best parameter for a particular symbol during particular time
        best meaning 
        a. strat performed consistent. break duration into x parts. each part should be similar
        b. strat outperform baseline price chg
        c. strat performed consistent when it is uptrend, downtrend, or flat.  
    
    --- final test result format
    strat_name | symbol | ...

    --- per strat per symbol result  
    start_date | end_date | trade_df_tf | indi_df_tf | ...params | baseline chg % | profit % 
    
    --- per strat per symbol rolling result for charting
    start_date | end_date | trade_df_tf | indi_df_tf | ...params | rolling_30d_start | rolling_30d_end | rolling baseline chg % | rolling profit % 
                                                                       2021-01-01    | 2021-02-01
                                                                       +1 week each row
    
    '''
    
    '''
    1. given start and end date. get rolling 30 days per 7 days.
    2. for list of symbol, retrieve printout result for quick analysis and rolling result for charting
    3.   
    '''    
    def _get_data(self, symbol, timeframe):
        conn = connect_to_db(DB_NAME, DB_HOST, DB_USERNAME, DB_PASSWORD)
        
        if timeframe == '1hour':
            table_name = 'binance_coin_hourly_historical_price'
        elif timeframe == '1day':
            table_name = 'binance_coin_historical_price'
        elif timeframe == '5mins':
            table_name = 'binance_coin_5mins_historical_price'
        elif timeframe == '4hours':
            table_name = 'binance_coin_4hours_historical_price'
        else:
            table_name = 'binance_coin_historical_price'   
        
        query = f"""
        SELECT * FROM {table_name}
        WHERE symbol = '{symbol}' 
        AND date BETWEEN '{self.start_date}' AND '{self.end_date}'
        ORDER BY date
        """
        df = pd.read_sql(query, conn)
        
        conn.close()
        return df

    def _get_rolling_date_list(self):
        total_days = (self.end_date - self.start_date).days
        train_days = int(total_days * self.train_test_split_pct)
        self.train_test_split_date = (self.start_date + timedelta(days=train_days)).strftime('%Y-%m-%d')
        
        rolling_dates = []
        current_date = self.start_date
        while current_date + timedelta(days=self.rolling_window) <= self.end_date:
            end_date = current_date + timedelta(days=self.rolling_window)
            rolling_dates.append((current_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')))
            current_date += timedelta(days=self.rolling_step)
        
        return rolling_dates
 
    def parameter_tuning(self, symbol, strat_name, trade_df_timeframe, indi_df_timeframe, extra_indi_df_timeframe, rolling_result=False):
        '''
        1. for a symbol+date range, run the strat with one param.
        2. receive the trades executed for that range and symbol  
        3. run trading_summary on total to get total
        4. run trading_summary separately on rolling date to get rolling data
        5. save the 2 results
        '''
        trade_df_timeframe = '1hour'
        indi_df_timeframe = '1day'
        trade_df = self._get_data(symbol, trade_df_timeframe) 
        indi_df = self._get_data(symbol, indi_df_timeframe) 
        extra_indi_df = None if extra_indi_df_timeframe is None else self._get_data(symbol, extra_indi_df_timeframe)
        
        results = []
        rolling_results = []
        
        # loop all parameters combo
        for params in itertools.product(*self.param_ranges.values()):
            param_dict = dict(zip(self.param_ranges.keys(), params))
            logging.info(f'running {param_dict}')
            ts = strat_name(
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
            
            # run once 
            ts.run_test()
             
            # get result
            trade_summary = ts.trading_summary() 
            if trade_summary:
                result = {
                    'start_date': self.start_date,
                    'end_date': self.end_date,
                    'trade_df_tf': trade_df_timeframe,  
                    'indi_df_tf': indi_df_timeframe, 
                    **param_dict,
                    'avg_trade_duration': trade_summary['Average Trade Duration'],
                    'median_trade_duration': trade_summary['Median Trade Duration'],
                    'total_trades': trade_summary['Total Number of Trades'],
                    'trades_win_rate': trade_summary['Trades Win Rate'],
                    'money_win_loss_ratio': trade_summary['Money Win/Loss Ratio'],
                    'baseline_chg_pct': trade_summary['key_metric_profit_pct'],
                    'profit_factor': trade_summary['key_metric_profit_pct'],
                    
                }
                results.append(result)
            else:
                logging.warning('No Trade Executed')
                return None
            
            # get rolling result
            if rolling_result:
                exec_df = ts.executions_df.copy()
                rolling_30d_start = []
                rolling_30d_end = []
                rolling_baseline_chg_pct = []
                rolling_profit_pct = []
                for start_date, end_date in self._get_rolling_date_list():
                    window_df = exec_df[(exec_df['execution_time'] >= start_date) & (exec_df['execution_time'] < end_date)]
                    if window_df.empty:
                        continue
                    result = ts.trading_summary(window_df)
                    rolling_30d_start.append(start_date)
                    rolling_30d_end.append(end_date)
                    rolling_baseline_chg_pct.append(result['Price Change Percent'])
                    rolling_profit_pct.append(result['key_metric_profit_pct'])
                
                rolling_results.append({
                    'start_date': self.start_date,
                    'end_date': self.end_date,
                    'trade_df_tf': trade_df_timeframe,  
                    'indi_df_tf': indi_df_timeframe,   
                    **param_dict,
                    'rolling_30d_start': rolling_30d_start,
                    'rolling_30d_end': rolling_30d_end,
                    'rolling_baseline_chg_pct': rolling_baseline_chg_pct,
                    'rolling_profit_pct': rolling_profit_pct
                })

        # Save all results to a CSV file
        results_df = pd.DataFrame(results)
        if rolling_result:
            rolling_results_df = pd.DataFrame(rolling_results)
        return results_df, rolling_results_df


param_ranges = {
    'profit_threshold': [10],
    'stoploss_threshold': [-0.05],
    'max_high_retrace': [0.05],
    'rsi_window': [7, 14],
    'rsi_window_2': [50],
    'rsi_sma_window': [20, 50, 100],
    'price_sma_window': [20, 50],
    'short_sma_window': [50],
    'long_sma_window': [200],
    'volume_short_sma_window': [14],
    'volume_long_sma_window': [30], 
    'atr_window': [10], 
    'kc_sma_window': [20], 
    'kc_mult': [2]
}

bit = strat_tuner(start_date='2021-01-01', 
                  end_date='2022-01-01', 
                  symbols=['BTC'], 
                  strat_names=StoneWellStrategy, 
                  param_ranges=param_ranges)

results_df, rolling_results_df = bit.parameter_tuning(
    symbol='BTC', 
    strat_name=StoneWellStrategy,
    trade_df_timeframe='1hour',
    indi_df_timeframe='1day',
    extra_indi_df_timeframe=None,
    rolling_result=True
)

results_df.to_csv('test_tuning_results.csv', index=False)
rolling_results_df.to_csv('test_tuning_rolling_results.csv', index=False)

# Read data from CSV files
# def parameter_tuning(trade_df, indi_df, strategy_class, param_ranges, extra_indi_df=None):
#     results = []
#     n = 1
    
#     for params in itertools.product(*param_ranges.values()):
#         param_dict = dict(zip(param_ranges.keys(), params))
#         logging.info(f'Begin trial {n}')
#         ts = strategy_class(
#             trade_candles_df=trade_df, 
#             indicator_candles_df=indi_df, 
#             executions_df=pd.DataFrame(), 
#             open_orders_df=pd.DataFrame(),
#             tlt_dollar=1000,
#             commission_pct=0.001,
#             extra_indicator_candles_df=extra_indi_df,
#             max_open_orders_per_symbol=1,
#             max_open_orders_total=3,
#             **param_dict
#         )
#         ts.run_test()
#         trade_summary = ts.trading_summary() 
        
#         result = {
#             'avg_trade_duration': trade_summary['Average Trade Duration'],
#             'median_trade_duration': trade_summary['Median Trade Duration'],
#             'total_trades': trade_summary['Total Number of Trades'],
#             'trades_win_rate': trade_summary['Trades Win Rate'],
#             'money_win_loss_ratio': trade_summary['Money Win/Loss Ratio'],
#             'profit_factor': trade_summary['key_metric_profit_pct'],
#             'buy_hold_profit': trade_summary['Buy and Hold Percent'],
#             **param_dict
#         }
#         results.append(result)
#         n += 1

#     # Save all results to a CSV file
#     results_df = pd.DataFrame(results)
#     return results_df

# TUNING_FOLDER ='./data/tuning_results/'
# param_ranges = {
#     'profit_threshold': [10],
#     'stoploss_threshold': [-0.05],
#     'rsi_window': [7, 14],
#     'rsi_sma_window': [20, 50, 100],
#     'price_sma_window': [20, 50],
#     'short_sma_window': [50],
#     'long_sma_window': [200],
#     'volume_short_sma_window': [14],
#     'volume_long_sma_window': [30], 
# }

# symbol='BTC'
# strat_name='stonewell'
# min_df_query = f'''
#     select *
#     from binance_coin_hourly_historical_price 
#     where symbol = '{symbol}'
#     and date between '2021-11-01' and '2024-01-01';
#     '''
    
# day_df_query = f'''
#     select *
#     from binance_coin_historical_price 
#     where symbol = '{symbol}'
#     and date between '2021-11-01' and '2024-01-01';
#     '''
# conn = connect_to_db(DB_NAME, DB_HOST, DB_USERNAME, DB_PASSWORD)
    
# min_df_chart = pd.read_sql(min_df_query, conn)
# day_df_chart = pd.read_sql(day_df_query, conn)

# results = parameter_tuning(min_df_chart, day_df_chart, 
#                            StoneWellStrategy, param_ranges,
#                            day_df_chart)

# results.to_csv(TUNING_FOLDER+f'{symbol}_{strat_name}_tuning_results_{datetime.now().strftime("%Y%m%d_%H%M")}.csv', index=False)



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
