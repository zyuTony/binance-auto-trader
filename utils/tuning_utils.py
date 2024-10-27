from datetime import datetime, timedelta
import itertools
import os
from dotenv import load_dotenv
import pandas as pd
import psycopg2
from psycopg2 import OperationalError
import logging

# from isolated_bn_data_db_updater.db_utils import *
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)8s | %(message)s',
    datefmt='%Y-%m-%d %H:%M' #datefmt='%Y-%m-%d %H:%M:%S'
)

load_dotenv()
class strat_tuner():
    
    def __init__(self, start_date, end_date, symbols, strat_name, param_ranges, trade_df_timeframe='1hour', indi_df_timeframe='1day', extra_indi_df_timeframe=None):
        
        self.db_host = os.getenv('RDS_ENDPOINT')
        self.db_name = 'financial_data'
        self.db_username = os.getenv('RDS_USERNAME')
        self.db_password = os.getenv('RDS_PASSWORD')
        
        self.start_date = datetime.strptime(start_date, '%Y-%m-%d')
        self.end_date = datetime.strptime(end_date, '%Y-%m-%d')
        self.symbols = symbols
        self.strat_name = strat_name
        self.param_ranges = param_ranges
        self.trade_df_timeframe = trade_df_timeframe
        self.indi_df_timeframe = indi_df_timeframe
        self.extra_indi_df_timeframe = extra_indi_df_timeframe
 
        self.rolling_window = 60
        self.rolling_step = 7  
    
    def connect_to_db(self):
        try:
            conn = psycopg2.connect(
                host = self.db_host,
                database = self.db_name,
                user = self.db_username,
                password = self.db_password)
            print(f"Connected to {self.db_name}!")
            return conn
        except OperationalError as e:
            print(f"{e}")
            return None
      
    def _get_data(self, symbol, timeframe):
        conn = self.connect_to_db()
        
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
        if '/' in symbol:
            base, quote = symbol.split('/')
            query = f"""
            SELECT a.date, a.open / b.open as open, a.high / b.high as high, 
                   a.low / b.low as low, a.close / b.close as close, 
                   -1 as volume, '{symbol}' as symbol
            FROM {table_name} a
            JOIN {table_name} b 
            ON a.date = b.date
            WHERE a.symbol = '{base}' AND b.symbol = '{quote}'
            AND a.date BETWEEN '{self.start_date}' AND '{self.end_date}'
            ORDER BY a.date
            """
        else:
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
        rolling_dates = []
        current_date = self.start_date
        while current_date + timedelta(days=self.rolling_window) <= self.end_date:
            end_date = current_date + timedelta(days=self.rolling_window)
            rolling_dates.append((current_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')))
            current_date += timedelta(days=self.rolling_step)
        
        return rolling_dates
    
    def _format_trades_n_charts(self, ts_object, start_date, end_date, trade_df_timeframe, indi_df_timeframe, param_dict, strat_name_str):
        '''This function formats the trades_df and exec_df to be inserted to db and update on frontend'''
        exec_df = ts_object.executions_df
        trade_df = ts_object.trade_candles_df

        # Add columns to exec_df
        exec_df['start_date'] = start_date
        exec_df['end_date'] = end_date
        exec_df['trade_df_tf'] = trade_df_timeframe
        exec_df['indi_df_tf'] = indi_df_timeframe
        exec_df['param_dict'] = exec_df.apply(lambda row: param_dict, axis=1)
        exec_df['strat_name'] = strat_name_str
        
        # Add columns to trade_df
        trade_df['start_date'] = start_date
        trade_df['end_date'] = end_date
        trade_df['trade_df_tf'] = trade_df_timeframe
        trade_df['indi_df_tf'] = indi_df_timeframe
        trade_df['param_dict'] = trade_df.apply(lambda row: param_dict, axis=1)
        trade_df['strat_name'] = strat_name_str

        # Transform exec_df
        exec_df = exec_df.rename(columns={'execution_time': 'date'})
        # exec_column_order = [
        #     'symbol', 'strat_name', 'start_date', 'end_date', 'trade_df_tf', 'indi_df_tf', 'param_dict', 
        #     'date', 'action', 'tlt_dollar', 'price']
        # exec_df = exec_df.reindex(columns=exec_column_order)
        
        
        # # Transform trade_df
        # trade_column_order = [
        #     'symbol', 'strat_name', 'start_date', 'end_date', 'trade_df_tf', 'indi_df_tf', 'param_dict', 
        #     'date', 'open', 'high', 'low', 'close', 'volume']
        # trade_df = trade_df.reindex(columns=trade_column_order)
        return exec_df, trade_df
    
    def _param_tune(self, symbol, strat_name):
        '''
        1. for a symbol+date range, run the strat with one param.
        2. receive the trades executed for that range and symbol  
        3. run trading_summary on total to get total
        4. run trading_summary separately on rolling date to get rolling data
        5. save the 2 results
        '''
        trade_df = self._get_data(symbol, self.trade_df_timeframe) 
        indi_df = self._get_data(symbol, self.indi_df_timeframe) 
        extra_indi_df = None if self.extra_indi_df_timeframe is None else self._get_data(symbol, self.extra_indi_df_timeframe)
 
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
                # Calculate baseline change percentage using start and end date close prices
                start_close = ts.trade_candles_df['close'].iloc[0]
                end_close = ts.trade_candles_df['close'].iloc[-1]
                baseline_chg_pct = (end_close - start_close) / start_close

                result = {
                    'symbol': symbol,
                    'strat_name': strat_name.__name__, 
                    'start_date': self.start_date,
                    'end_date': self.end_date,
                    'trade_df_tf': self.trade_df_timeframe,  
                    'indi_df_tf': self.indi_df_timeframe, 
                    'param_dict': param_dict,  
                    'avg_trade_duration': trade_summary['Average Trade Duration'],
                    'median_trade_duration': trade_summary['Median Trade Duration'],
                    'total_trades': trade_summary['Total Number of Trades'],
                    'trades_win_rate': trade_summary['Trades Win Rate'],
                    'money_win_loss_ratio': trade_summary['Money Win/Loss Ratio'],
                    'baseline_chg_pct': baseline_chg_pct,
                    'profit_factor': trade_summary['key_metric_profit_pct'],
                }
                results.append(result)
            else:
                logging.warning('No Trade Executed')
                return None
            
            print(param_dict)
            # format trades and charts for display on website
            trades_df, charts_df = self._format_trades_n_charts(ts, self.start_date, self.end_date, 
                                         self.trade_df_timeframe, self.indi_df_timeframe, 
                                         param_dict, strat_name.__name__)
            
            # get rolling returns for display on website
            exec_df = ts.executions_df.copy()
            for start_date, end_date in self._get_rolling_date_list():
                window_df = exec_df[(exec_df['execution_time'] >= start_date) & (exec_df['execution_time'] < end_date)]
                if not window_df.empty:
                    result = ts.trading_summary(window_df)
                else:
                    # Default values for result if no data
                    result = { 'key_metric_profit_pct': 0 }
                
                # Calculate baseline change percentage using start and end date close prices
                start_close = ts.trade_candles_df[ts.trade_candles_df['date'] == start_date]['close'].values[0]
                end_close = ts.trade_candles_df[ts.trade_candles_df['date'] == end_date]['close'].values[0]
                baseline_chg_pct = (end_close - start_close) / start_close  
                
                rolling_results.append({
                    'symbol': symbol,
                    'strat_name': strat_name.__name__,  # Save the strategy name as a string
                    'start_date': self.start_date,
                    'end_date': self.end_date,
                    'trade_df_tf': self.trade_df_timeframe,  
                    'indi_df_tf': self.indi_df_timeframe,   
                    'param_dict': param_dict,  # Store param_dict as a single entry
                    'rolling_30d_start': start_date,
                    'rolling_30d_end': end_date,
                    'rolling_baseline_chg_pct': baseline_chg_pct,
                    'rolling_profit_pct': result['key_metric_profit_pct'],
                    'start_close': start_close,
                    'end_close': end_close
                })
        # Save all results to a CSV file
        results_df = pd.DataFrame(results)
        rolling_results_df = pd.DataFrame(rolling_results)
        
        return trades_df, charts_df, results_df, rolling_results_df

    def multi_symbols_param_tuning(self):
        all_results = []
        all_rolling_results = []
        all_trades = []
        all_charts = []
        
        for symbol in self.symbols:
            logging.info(f"Running parameter tuning for symbol: {symbol}")
            trades_df, charts_df, results_df, rolling_results_df = self._param_tune(
                symbol=symbol,
                strat_name=self.strat_name
            )
            
            all_results.append(results_df)
            all_rolling_results.append(rolling_results_df)
            all_trades.append(trades_df)
            all_charts.append(charts_df)
        
        # Combine results for all symbols
        combined_results = pd.concat(all_results, ignore_index=True)
        combined_rolling_results = pd.concat(all_rolling_results, ignore_index=True)
        combined_trades = pd.concat(all_trades, ignore_index=True)
        combined_charts = pd.concat(all_charts, ignore_index=True)
        
        return combined_trades, combined_charts, combined_results, combined_rolling_results

         