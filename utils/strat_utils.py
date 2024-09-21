import pandas as pd
from abc import ABC, abstractmethod
from binance.client import Client
from utils.trading_utils import *
from binance.enums import *
from binance.helpers import round_step_size
import requests
import os 
from dotenv import load_dotenv
from io import StringIO
from tqdm import tqdm
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)8s | %(message)s',
    datefmt='%Y-%m-%d %H:%M' #datefmt='%Y-%m-%d %H:%M:%S'
)

def avan_daily_stock_data_as_csv(ticker, avan_api_key, outputsize, num_rows=None):
    url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={ticker}&outputsize={outputsize}&datatype=csv&apikey={avan_api_key}'
    response = requests.get(url)
    if response.status_code == 200:
        df = pd.read_csv(StringIO(response.text))
        df = df.rename(columns={'timestamp': 'date'})
        df['date'] = pd.to_datetime(df['date'])
        df['symbol'] = ticker  # Add symbol column after date
        df = df.sort_values('date', ascending=True)  # Sort in ascending order first
        if num_rows is not None:
            df = df.tail(num_rows)  # Get only the last num_rows
        logging.info(f"{ticker} daily retrieved!")
        return df.reset_index(drop=True)
    else:
        logging.error(f"Error fetching {ticker}: {response.status_code}")
        return None
        
def avan_intraday_stock_data_as_csv(interval, months, ticker, avan_api_key, outputsize):
    all_data = []
    for month in months:
        url = f'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol={ticker}&interval={interval}&month={month}&outputsize={outputsize}&datatype=csv&apikey={avan_api_key}'
        response = requests.get(url)
        if response.status_code == 200:
            df = pd.read_csv(StringIO(response.text))
            df = df.rename(columns={'timestamp': 'date'})
            df['date'] = pd.to_datetime(df['date'])
            df['symbol'] = ticker  # Add symbol column after date
            all_data.append(df)
            logging.info(f"{ticker} intraday data for {month} retrieved!")
        else:
            logging.error(f"Error fetching {ticker} for {month}: {response.status_code}")
    
    if all_data:
        combined_df = pd.concat(all_data, ignore_index=True)
        combined_df = combined_df.sort_values('date', ascending=True)
        combined_df = combined_df.drop_duplicates(subset=['date'], keep='first')
        logging.info(f"Combined intraday data for {ticker} retrieved!")
        return combined_df.reset_index(drop=True)
    else:
        logging.warning(f"No data retrieved for {ticker}")
        return None

'''grandparents'''  
class Strategy(ABC):
    
    def __init__(self, trade_candles_df, indicator_candles_df, executions_df, open_orders_df, tlt_dollar, commission_pct, extra_indicator_candles_df, profit_threshold, stoploss_threshold, max_high_retrace, max_open_orders_per_symbol, max_open_orders_total):
        self.trade_candles_df = trade_candles_df
        self.indicator_candles_df = indicator_candles_df
        self.extra_indicator_candles_df = extra_indicator_candles_df
        self.executions_df = executions_df
        self.open_orders_df = open_orders_df
        self.tlt_dollar = tlt_dollar
        self.profit_threshold = profit_threshold
        self.stoploss_threshold = stoploss_threshold
        self.max_high_retrace = max_high_retrace
        self.commission_pct = commission_pct
        self.max_open_orders_per_symbol = max_open_orders_per_symbol
        self.max_open_orders_total = max_open_orders_total
        
    def _check_candle_frequency(self, df):
        # Check if 'date' column exists
        if 'date' not in df.columns:
            raise ValueError("DataFrame must have a 'date' column")

        # Convert 'date' column to datetime if it's not already
        df['date'] = pd.to_datetime(df['date'])

        # Calculate the most common time difference between consecutive rows
        time_diff = df['date'].diff().mode().iloc[0]

        # Determine the frequency based on the time difference
        if time_diff == pd.Timedelta(minutes=1):
            return "1 Minute data"
        elif time_diff == pd.Timedelta(days=1):
            return "Daily data"
        elif time_diff == pd.Timedelta(days=7):
            return "Weekly data"
        elif time_diff == pd.Timedelta(minutes=5):
            return "5 Minute data"
        elif time_diff == pd.Timedelta(minutes=10):
            return "10 Minute data"
        elif time_diff == pd.Timedelta(hours=1):
            return "1 Hour data"
        elif time_diff == pd.Timedelta(hours=2):
            return "2 Hour data"
        elif time_diff == pd.Timedelta(hours=4):
            return "4 Hour data"
        elif time_diff == pd.Timedelta(hours=12):
            return "12 Hour data"
        else:
            return f"Unknown frequency: {time_diff}"

    def _update_open_orders_logs(self, last_update_time, status, symbol, tlt_dollar, price, quantity, high_since_open): 
        new_order = {
            'last_update_time': last_update_time, 
            'status': status, 
            'symbol': symbol, 
            'tlt_dollar': tlt_dollar, 
            'price': price, 
            'quantity': quantity,
            'high_since_open': high_since_open}
        self.open_orders_df = pd.concat([self.open_orders_df, pd.DataFrame([new_order])], ignore_index=True)
  
    def _update_execution_logs(self, execution_time, action, symbol, tlt_dollar, price, quantity):   
        new_exec = {
            'execution_time': execution_time,
            'action': action,
            'symbol': symbol,
            'tlt_dollar': tlt_dollar,
            'price': price,
            'quantity': quantity
        }
        self.executions_df = pd.concat([self.executions_df, pd.DataFrame([new_exec])], ignore_index=True)

    @abstractmethod
    def stepwise_logic_open(self):  # determine whether open
        pass

    @abstractmethod
    def stepwise_logic_close(self): # determine whether close
        pass
    
    @abstractmethod
    def get_indicators(self, df):
        pass
    
    @abstractmethod
    def get_extra_indicators(self, df):
        pass
    
    @abstractmethod
    def buy(self, tlt_dollar, execution_time, symbol, price, quantity):  # different between test and real
        pass

    @abstractmethod
    def sell(self, quantity, execution_time, symbol, tlt_dollar, price):   # different between test and real
        pass


'''parents'''
class TestStrategy(Strategy):

    def buy(self, tlt_dollar, execution_time, symbol, price, quantity): 
        self._update_execution_logs(execution_time, 'BUY', symbol, tlt_dollar, price, quantity)
        return {'executed_time_str': execution_time,
                'executed_quantity': quantity,
                'executed_tlt_dollar': tlt_dollar,
                'executed_price': price}
            
    def sell(self, quantity, execution_time, symbol, tlt_dollar, price):  
        self._update_execution_logs(execution_time, 'SELL', symbol, tlt_dollar, price, quantity)
        return {'executed_time_str': execution_time,
                'executed_quantity': quantity,
                'executed_tlt_dollar': tlt_dollar,
                'executed_price': price}
    
    def short_sell(self, quantity, execution_time, symbol, tlt_dollar, price):  
        self._update_execution_logs(execution_time, 'SHORT_SELL', symbol, tlt_dollar, price, quantity)
        return {'executed_time_str': execution_time,
                'executed_quantity': quantity,
                'executed_tlt_dollar': tlt_dollar,
                'executed_price': price}
        
    def short_close(self, tlt_dollar, execution_time, symbol, price, quantity):  
        self._update_execution_logs(execution_time, 'SHORT_CLOSE', symbol, tlt_dollar, price, quantity)
        return {'executed_time_str': execution_time,
                'executed_quantity': quantity,
                'executed_tlt_dollar': tlt_dollar,
                'executed_price': price}
        
    def close_all_trades(self):
        num_open_trades = 0
        for index, order in self.open_orders_df.iterrows():
            if order['status'] == 'OPEN': 
                last_candle = self.indicator_candles_df.iloc[-1]
                
                current_price = last_candle['close']
                execution_time = last_candle['date']
                symbol = order['symbol']
                
                quantity = order['quantity']
                tlt_dollar = current_price * quantity
                
                self.sell(quantity, execution_time, symbol, tlt_dollar, current_price)
                self.open_orders_df.at[index, 'status'] = 'CLOSED'
                self.open_orders_df.at[index, 'close_reason'] = 'close all'
                num_open_trades += 1
        logging.info(f'Closed all {num_open_trades} remaining open trades!')
    
    def run_test(self): 
        '''This function will join the trading tf with the indicators tf. The indicator will lag the trade by one day/hour to mimic real trading scenario.'''
        # check df frequencies
        trade_df_timestamp = self._check_candle_frequency(self.trade_candles_df)
        indi_df_timestamp = self._check_candle_frequency(self.indicator_candles_df)
        extra_indi_df_timestamp = self._check_candle_frequency(self.extra_indicator_candles_df) if self.extra_indicator_candles_df is not None else indi_df_timestamp
        
        # get indicators
        self.indicator_candles_df = self.get_indicators(self.indicator_candles_df)
        self.extra_indicator_candles_df = self.get_extra_indicators(self.extra_indicator_candles_df) if self.extra_indicator_candles_df is not None else None
        
        # transform based on timeframe of the dataframes
        # deal with hourly trade, hourly indi, hourly extra indi 
        if trade_df_timestamp == indi_df_timestamp == extra_indi_df_timestamp:
            # Shift indicator dataframes by one row
            shifted_indicator_df = self.indicator_candles_df.shift(1)
            shifted_indicator_df['date'] = self.indicator_candles_df['date']
            
            self.trade_candles_df = pd.merge(self.trade_candles_df, shifted_indicator_df, on='date', how='left', suffixes=('', '_indi'))
            
            if self.extra_indicator_candles_df is not None:
                shifted_extra_indicator_df = self.extra_indicator_candles_df.shift(1)
                shifted_extra_indicator_df['date'] = self.extra_indicator_candles_df['date']
                
                self.trade_candles_df = pd.merge(self.trade_candles_df, shifted_extra_indicator_df, on='date', how='left', suffixes=('', '_exindi'))
        # deal with hourly trade, daily indi, daily extra indi         
        elif trade_df_timestamp != indi_df_timestamp and indi_df_timestamp == extra_indi_df_timestamp and indi_df_timestamp=="Daily data":
            self.trade_candles_df['date_day'] = self.trade_candles_df['date'].dt.date
            indicator_candles_df_shifted = self.indicator_candles_df.copy()
            indicator_candles_df_shifted['date'] = pd.to_datetime(indicator_candles_df_shifted['date']).dt.date + pd.Timedelta(days=1)
            self.trade_candles_df = self.trade_candles_df.merge(indicator_candles_df_shifted, left_on='date_day', right_on='date', suffixes=('', '_indi'), how='left')
            
            if self.extra_indicator_candles_df is not None:
                extra_indicator_candles_df_shifted = self.extra_indicator_candles_df.copy()
                extra_indicator_candles_df_shifted['date'] = pd.to_datetime(extra_indicator_candles_df_shifted['date']).dt.date + pd.Timedelta(days=1)
                self.trade_candles_df = self.trade_candles_df.merge(extra_indicator_candles_df_shifted, left_on='date_day', right_on='date', suffixes=('', '_exindi'), how='left')
        # deal with hourly trade, hourly indi, daily extra indi 
        elif trade_df_timestamp == indi_df_timestamp and indi_df_timestamp != extra_indi_df_timestamp and extra_indi_df_timestamp=="Daily data": 
            shifted_indicator_df = self.indicator_candles_df.shift(1)
            shifted_indicator_df['date'] = self.indicator_candles_df['date']
            self.trade_candles_df = pd.merge(self.trade_candles_df, shifted_indicator_df, on='date', how='left', suffixes=('', '_indi'))
            
            if self.extra_indicator_candles_df is not None:
                self.trade_candles_df['date_day'] = self.trade_candles_df['date'].dt.date
                extra_indicator_candles_df_shifted = self.extra_indicator_candles_df.copy()
                extra_indicator_candles_df_shifted['date'] = pd.to_datetime(extra_indicator_candles_df_shifted['date']).dt.date + pd.Timedelta(days=1)
                self.trade_candles_df = self.trade_candles_df.merge(extra_indicator_candles_df_shifted, left_on='date_day', right_on='date', suffixes=('', '_exindi'), how='left')

        else:
            logging.error(f"problem with indicator timeframe: trade:{trade_df_timestamp}, indi:{indi_df_timestamp}, extra indi:{extra_indi_df_timestamp}")
            return -1
        
        logging.debug('All Columns in trading df: %s', self.trade_candles_df.columns) 
        # go through the all trade df row by row 
        offset = 4
        for idx in tqdm(range(offset, len(self.trade_candles_df))):
            start_idx = idx - offset
            candle_df_slices = self.trade_candles_df.iloc[start_idx:idx+1]
            
            # opening 
            self.stepwise_logic_open(candle_df_slices)
            # closing
            for order_index, open_order in self.open_orders_df.iterrows():
                if open_order['status'] == 'OPEN' and open_order['symbol'] == candle_df_slices.iloc[-1]['symbol']:
                    self.stepwise_logic_close(candle_df_slices, order_index)
        
        # wrap up all trades
        self.close_all_trades()        
        logging.info(f'Finished test run!')

    def trading_summary(self, df=None):
        df = self.executions_df if df is None else df
        if df.empty:
            logging.warning("No trades executed! No Summary")
            return None

        # Calculate profit for each trade
        df['trade_profit'] = 0.0
        df['trade_duration'] = pd.Timedelta(0)

        buy_stack = []
        total_profit = 0.0
        total_trades = 0
        total_volume = 0.0

        for _, row in df.iterrows():
            if row['action'] == 'BUY':
                buy_stack.append(row)
                total_volume += row['tlt_dollar']
            elif row['action'] == 'SELL':
                if buy_stack:
                    buy_order = buy_stack.pop(0)
                    profit = row['tlt_dollar'] - buy_order['tlt_dollar']
                    df.loc[row.name, 'trade_profit'] = profit
                    trade_duration = row['execution_time'] - buy_order['execution_time']
                    df.loc[row.name, 'trade_duration'] = trade_duration
                    total_profit += profit
                    total_trades += 1
                    total_volume += row['tlt_dollar']

        total_commission = total_volume * self.commission_pct
        
        # Calculate price change
        first_price = df.iloc[0]['price']
        last_price = df.iloc[-1]['price']
        price_change = last_price - first_price
        price_change_percent = ((last_price - first_price) / first_price) 
        
        # Calculate win rate
        profitable_trades = df[df['trade_profit'] > 0]
        win_rate = len(profitable_trades) / total_trades if total_trades > 0 else 0

        # Calculate P&L
        total_money_made = df[df['trade_profit'] > 0]['trade_profit'].sum()
        total_money_lost = abs(df[df['trade_profit'] < 0]['trade_profit'].sum())

        # Calculate trade duration statistics
        trade_durations = df[df['trade_duration'] > pd.Timedelta(0)]['trade_duration']
        avg_trade_duration = trade_durations.mean()
        median_trade_duration = trade_durations.median()

        summary = {
            "Total Number of Trades": total_trades,
            "Total Profit": f"${total_profit:.0f}",
            "Total Trading Volume": f"${total_volume:.0f}",
            "Profit per Trade": f"${total_profit/total_trades:.2f}" if total_trades > 0 else "$0.00",
            "% Profit per Trade": f"{(total_profit/total_trades)/self.tlt_dollar:.2%}" if total_trades > 0 else "0.00%",
            "Total Commission Cost": f"${total_commission:.2f}",
            "Price Change": f"${price_change:.2f}",
            "Price Change Percent": f"{price_change_percent:.2f}",
            "Trades Win Rate": f"{win_rate:.1%}", 
            "Total Money Made": f"${total_money_made:.0f}",
            "Total Money Lost": f"${total_money_lost:.0f}",
            "Money Win/Loss Ratio": f"{total_money_made / total_money_lost:.1f}" if total_money_lost != 0 else "N/A",
            "Average Trade Duration": str(avg_trade_duration),
            "Median Trade Duration": str(median_trade_duration),
            "key_metric_profit_pct": round((total_profit/total_trades)/self.tlt_dollar,5) if total_trades > 0 else 0
        }
        
        if summary:
            for key, value in summary.items():
                print(f"{key}: {value}")
        
        return summary

load_dotenv()
api_key = os.getenv('BINANCE_API')
api_secret = os.getenv('BINANCE_SECRET')
avan_api_key = os.getenv('ALPHA_VANTAGE_PREM_API') 
bn_client = Client(api_key, api_secret)

class BinanceProductionStrategy(Strategy):
    def __init__(self, *args, ideal_executions_df, **kwargs):
        super().__init__(*args, **kwargs)
        self.bn_client = bn_client
        self.commission_pct = 0
        self.ideal_executions_df = ideal_executions_df
    
    def _update_ideal_execution_logs(self, execution_time, action, symbol, tlt_dollar, price, quantity):   
        new_exec = {
            'execution_time': execution_time,
            'action': action,
            'symbol': symbol,
            'tlt_dollar': tlt_dollar,
            'price': price,
            'quantity': quantity
        }
        self.ideal_executions_df = pd.concat([self.ideal_executions_df, pd.DataFrame([new_exec])], ignore_index=True)
    
    @staticmethod
    def _get_tick_size(price):
        '''helper function to get the decimal right for trading'''
        if price < 5:
            tick_size = 1
        elif price >=5 and price < 100:
            tick_size = 0.01
        elif price >=100 and price < 2000:
            tick_size = 0.001  
        elif price >=1000 and price < 10000:
            tick_size = 0.0001 
        elif price >=10000:
            tick_size = 0.00001 
        return tick_size
        
    def buy(self, tlt_dollar, execution_time, symbol, price, quantity): 
        # execute buy with a calculated decimal precision amount
        try:
            order_info = self.bn_client.order_market_buy(symbol=symbol, 
                                                         quantity=round_step_size(quantity, self._get_tick_size(price)))
        except Exception as e:
            logging.error(f"Error executing buy order: {str(e)}")
            return None
        
        # log order
        executed_time_str = datetime.fromtimestamp(order_info['transactTime'] / 1000).strftime('%Y-%m-%d %H:%M:%S')
        executed_quantity = float(order_info['executedQty'])
        executed_tlt_dollar = float(order_info['cummulativeQuoteQty'])
        executed_price = executed_tlt_dollar/executed_quantity
        
        self._update_execution_logs(executed_time_str, 'BUY', 
                                    symbol, executed_tlt_dollar, 
                                    executed_price, executed_quantity)
        
        self._update_ideal_execution_logs(execution_time, 'BUY', 
                                    symbol, tlt_dollar, 
                                    price, quantity)
        
        logging.info(f'longed {symbol}. bought {executed_quantity} of them of ${executed_tlt_dollar}!')
        return {'executed_time_str': executed_time_str,
                'executed_quantity': executed_quantity,
                'executed_tlt_dollar': executed_tlt_dollar,
                'executed_price': executed_price}
         
    def sell(self, quantity, execution_time, symbol, tlt_dollar, price):  
        balance_amt = float(self.bn_client.get_asset_balance(
                    asset=symbol.replace('USDT', ''))['free'])
        if quantity > balance_amt: # make sure have enough to sell
            raise ValueError(f"Insufficient balance. Attempted to sell {quantity} {symbol}, but only {balance_amt} available.")
        
        try:
            order_info = self.bn_client.order_market_sell(symbol=symbol, 
                                                         quantity=round_step_size(quantity, self._get_tick_size(price)))
        except Exception as e:
            logging.error(f"Error executing sell order: {str(e)}")
            return None
        
        # log order
        executed_time_str = datetime.fromtimestamp(order_info['transactTime'] / 1000).strftime('%Y-%m-%d %H:%M:%S')
        executed_quantity = float(order_info['executedQty'])
        executed_tlt_dollar = float(order_info['cummulativeQuoteQty'])
        executed_price = executed_tlt_dollar/executed_quantity
        
        self._update_execution_logs(executed_time_str, 'SELL', 
                                    symbol, executed_tlt_dollar, 
                                    executed_price, executed_quantity)
        
        self._update_ideal_execution_logs(execution_time, 'SELL', 
                                    symbol, tlt_dollar, 
                                    price, quantity)
        
        logging.info(f'Sold {symbol}, {quantity} of them.')
        return {'executed_time_str': executed_time_str,
                'executed_quantity': executed_quantity,
                'executed_tlt_dollar': executed_tlt_dollar,
                'executed_price': executed_price}
        
    def sell_all(self, execution_time, symbol, tlt_dollar, price):  
        balance_amt = float(self.bn_client.get_asset_balance(
                    asset=symbol.replace('USDT', ''))['free'])
        order_info = self.bn_client.order_market_sell(symbol=symbol, 
                                                     quantity=round_step_size(balance_amt, self._get_tick_size(price)))
        
        # log order
        executed_time_str = datetime.fromtimestamp(order_info['transactTime'] / 1000).strftime('%Y-%m-%d %H:%M:%S')
        executed_quantity = float(order_info['executedQty'])
        executed_tlt_dollar = float(order_info['cummulativeQuoteQty'])
        executed_price = executed_tlt_dollar/executed_quantity
        
        self._update_execution_logs(executed_time_str, 'SELL', 
                                    symbol, executed_tlt_dollar, 
                                    executed_price, executed_quantity)
        
        self._update_ideal_execution_logs(execution_time, 'SELL', 
                                    symbol, tlt_dollar, 
                                    price, balance_amt)
        
        logging.info(f'Sold ALL {symbol}, {balance_amt} of them.')
    
    def run_once(self): 
        # check df frequencies
        trade_df_timestamp = self._check_candle_frequency(self.trade_candles_df)
        indi_df_timestamp = self._check_candle_frequency(self.indicator_candles_df)
        extra_indi_df_timestamp = self._check_candle_frequency(self.extra_indicator_candles_df) if self.extra_indicator_candles_df is not None else indi_df_timestamp
        
        # get indicators
        self.indicator_candles_df = self.get_indicators(self.indicator_candles_df)
        self.extra_indicator_candles_df = self.get_extra_indicators(self.extra_indicator_candles_df) if self.extra_indicator_candles_df is not None else None
        
        # transform based on timeframe of the dataframes
        if trade_df_timestamp == indi_df_timestamp == extra_indi_df_timestamp:
            self.trade_candles_df = pd.merge(self.trade_candles_df, self.indicator_candles_df, on='date', how='left', suffixes=('', '_indi'))
            if self.extra_indicator_candles_df is not None:
                self.trade_candles_df = pd.merge(self.trade_candles_df, self.extra_indicator_candles_df, on='date', how='left', suffixes=('', '_exindi'))
                
        elif indi_df_timestamp == extra_indi_df_timestamp and trade_df_timestamp != indi_df_timestamp:
            self.trade_candles_df['date_day'] = self.trade_candles_df['date'].dt.date
            self.indicator_candles_df['date'] = pd.to_datetime(self.indicator_candles_df['date']).dt.date
            self.trade_candles_df = self.trade_candles_df.merge(self.indicator_candles_df, left_on='date_day', right_on='date', suffixes=('', '_indi'), how='left')
            
            if self.extra_indicator_candles_df is not None:
                self.extra_indicator_candles_df['date'] = pd.to_datetime(self.extra_indicator_candles_df['date']).dt.date
                self.trade_candles_df = self.trade_candles_df.merge(self.extra_indicator_candles_df, left_on='date_day', right_on='date', suffixes=('', '_exindi'), how='left')
                   
        else:
            logging.warning("Indicator and extra indicator dataframes have different timeframes. Will not run production.")
            return -1
 
        candle_df_slice = self.trade_candles_df.iloc[-1]
        # opening 
        self.stepwise_logic_open(candle_df_slice)
        # closing
        for order_index, open_order in self.open_orders_df.iterrows():
            if open_order['status'] == 'OPEN' and open_order['symbol'] == candle_df_slice['symbol']:
                self.stepwise_logic_close(candle_df_slice, order_index)     
        logging.info(f'Finished runinng once for latest data!')

'''kids'''
class StoneWellStrategy(TestStrategy):
    '''
    INDICATORS: 
    SMA20 of stock; RSI14; SMA20 of RSI14; SMA10/20 of volume 
    BUY -> 
    1) close >= SMA20D 
    2) RSI14D > SMA20D(RSI14D) 
    3) SMA50D <= SMA200D 
    4) SMA10D(vol) > SMA20D(vol)
    
    SELL/CLOSE -> 
    when one of the 4 above stop being true
    
    STOP lOSS -> 10%
    ''' 
    def __init__(self, *args, rsi_window, rsi_window_2, rsi_sma_window, price_sma_window, 
                 short_sma_window, long_sma_window, volume_short_sma_window, 
                 volume_long_sma_window, atr_window, kc_sma_window, kc_mult, **kwargs):
        super().__init__(*args, **kwargs)
        self.rsi_window = rsi_window
        self.rsi_window_2 = rsi_window_2
        self.rsi_sma_window = rsi_sma_window
        self.price_sma_window = price_sma_window
        self.short_sma_window = short_sma_window
        self.long_sma_window = long_sma_window
        self.volume_short_sma_window = volume_short_sma_window
        self.volume_long_sma_window = volume_long_sma_window
        self.atr_window = atr_window
        self.kc_sma_window = kc_sma_window
        self.kc_mult = kc_mult
        
    def get_indicators(self, df):
        # Calculate RSI
        delta = df['close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=self.rsi_window).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=self.rsi_window).mean()
        rs = gain / loss
        df['RSI'] = 100 - (100 / (1 + rs))

        # Calculate SMA20 of RSI14
        df['RSI_SMA'] = df['RSI'].rolling(window=self.rsi_sma_window).mean()
        
        # Calculate RSI 2 
        gain_2 = (delta.where(delta > 0, 0)).rolling(window=self.rsi_window_2).mean()
        loss_2 = (-delta.where(delta < 0, 0)).rolling(window=self.rsi_window_2).mean()
        rs_2 = gain_2 / loss_2
        df['RSI_2'] = 100 - (100 / (1 + rs_2))

        # Calculate SMA20 of stock
        df['close_SMA'] = df['close'].rolling(window=self.price_sma_window).mean()
        df['close_short_SMA'] = df['close'].rolling(window=self.short_sma_window).mean()
        df['close_long_SMA'] = df['close'].rolling(window=self.long_sma_window).mean()

        # Calculate SMA10 and SMA20 of volume
        df['volume_short_SMA'] = df['volume'].rolling(window=self.volume_short_sma_window).mean()
        df['volume_long_SMA'] = df['volume'].rolling(window=self.volume_long_sma_window).mean()

        # Calculate EMA 12 and 26
        df['EMA_12'] = df['close'].ewm(span=12, adjust=False).mean()
        df['EMA_26'] = df['close'].ewm(span=26, adjust=False).mean()

         # Calculate Average True Range (ATR)
        df['high_low'] = df['high'] - df['low']
        df['high_close'] = abs(df['high'] - df['close'].shift())
        df['low_close'] = abs(df['low'] - df['close'].shift())
        df['true_range'] = pd.concat([df['high_low'], df['high_close'], df['low_close']], axis=1).max(axis=1)
        df['ATR'] = df['true_range'].rolling(window=self.atr_window).mean()

        # Calculate Keltner Channels
        df['KC_middle'] = df['close'].rolling(window=self.kc_sma_window).mean()
        df['KC_upper'] = df['KC_middle'] + (df['ATR'] * self.kc_mult)
        df['KC_lower'] = df['KC_middle'] - (df['ATR'] * self.kc_mult)
        # Calculate KC_position
        df['KC_position'] = (df['close'] - df['KC_lower']).clip(lower=0) / (df['KC_upper'] - df['KC_lower'])
        return df
    
    def get_extra_indicators(self, df):
        return df
        
    def stepwise_logic_open(self, trade_candle_df_slices): 
        curr_candle = trade_candle_df_slices.iloc[-1]
        prev_candle = trade_candle_df_slices.iloc[-2]
        # LOGIC 
        if (
            True
            and curr_candle['RSI'] > curr_candle['RSI_2']
            and curr_candle['KC_position'] <= 0.5
            # and (curr_candle['KC_position'] <= 0.5
            # or (curr_candle['ATR']/curr_candle['close_SMA'] < 0.008))
            # and curr_candle['EMA_12'] > curr_candle['EMA_26']
            # and (prev_candle['RSI_SMA'] ) < (curr_candle['RSI_SMA'])
            # and curr_candle['RSI'] > curr_candle['RSI_SMA']  # bullish rsi
            and curr_candle['open'] > curr_candle['close_SMA'] # daily price > SMA
            # and curr_candle['close_short_SMA'] < curr_candle['close_long_SMA'] # death crossed waiting for golden cross
            # and curr_candle['volume_short_SMA'] > curr_candle['volume_long_SMA'] # volume short term bullish
            # and curr_candle['volume'] > curr_candle['volume_long_SMA'] 
        ):

            last_update_time = execution_time = curr_candle['date']
            
            # Check if there's more than max open order
            if (self.open_orders_df.empty or 
                (len(self.open_orders_df[self.open_orders_df['status'] == 'OPEN']) < self.max_open_orders_total and
                 len(self.open_orders_df[(self.open_orders_df['status'] == 'OPEN') & 
                                         (self.open_orders_df['symbol'] == curr_candle['symbol'])]) < self.max_open_orders_per_symbol)):
                symbol = curr_candle['symbol']  
                price = high_since_open = curr_candle['open']
                quantity = self.tlt_dollar / price    
                 
                self.buy(self.tlt_dollar*(1+self.commission_pct), execution_time, symbol, price, quantity)
                self._update_open_orders_logs(last_update_time, 'OPEN', symbol, self.tlt_dollar, price, quantity, high_since_open)
                logging.info(f'{last_update_time}: Opened position at {price:.2f}')
            else:
                logging.debug(f"{last_update_time}: Position already open, skipping new order")
        else:
            logging.debug(f"{curr_candle['date']}: opening stand by")
            
    def stepwise_logic_close(self, trade_candle_df_slices, order_index):
        order = self.open_orders_df.iloc[order_index]
        open_price = order['price'] 
        curr_candle = trade_candle_df_slices.iloc[-1]
        prev_candle = trade_candle_df_slices.iloc[-2]
        current_price = curr_candle['open'] 
        profit_percentage = (current_price - open_price) / open_price  
        
        # Update high_since_open using current price
        self.open_orders_df.at[order_index, 'high_since_open'] = max(current_price, order['high_since_open'])
        high_since_open = self.open_orders_df.at[order_index, 'high_since_open']
        
        close_reason = ""
        if profit_percentage <= self.stoploss_threshold:
            close_reason = f"Stop loss hit (P%: {profit_percentage:.1%})"
        elif profit_percentage >= self.profit_threshold:
            close_reason = f"Profit target met (P%: {profit_percentage:.1%})"
        # elif curr_candle['RSI'] <= curr_candle['RSI_2'] and curr_candle['EMA_12'] <= curr_candle['EMA_26']:
        #     close_reason = f"RSI and MACD bearish"
        # elif curr_candle['RSI'] < curr_candle['RSI_2']:
        #     close_reason = f"Short RSI < Long RSI"
        # elif curr_candle['EMA_12'] <= curr_candle['EMA_26']:
        #     close_reason = f"MACD crossed below"
        # elif curr_candle['KC_position'] > 1.3:
        #     close_reason = f"KC position > 1.3"
        elif current_price <= high_since_open * (1 - self.max_high_retrace) and profit_percentage > 0:
            close_reason = f"Price retraced but profitable"
        # elif curr_candle['volume'] < curr_candle['volume_long_SMA']:
        #     close_reason = f"Volume < Long SMA"
        # elif (prev_candle['RSI_SMA']) > (curr_candle['RSI_SMA']):
        #     close_reason = f"RSI SMA decreased"
        # elif curr_candle['RSI'] <= curr_candle['RSI_SMA']:
        #     close_reason = f"RSI < RSI SMA"
        # elif curr_candle['open'] < curr_candle['close_SMA']:
        #     close_reason = f"Price < SMA"
        # elif curr_candle['close_short_SMA'] >= curr_candle['close_long_SMA']:
        #     close_reason = f"Short SMA > Long SMA"
        # elif curr_candle['volume_short_SMA'] <= curr_candle['volume_long_SMA']:
        #     close_reason = f"Vol Short SMA < Long SMA"
        if close_reason:
            execution_time = curr_candle['date']
            symbol = order['symbol']
            price = curr_candle['open']
            quantity = order['quantity']
            tlt_dollar = price * quantity
            self.sell(quantity, execution_time, symbol, tlt_dollar*(1-self.commission_pct), current_price)
            self.open_orders_df.at[order_index, 'status'] = 'CLOSED'
            self.open_orders_df.at[order_index, 'close_reason'] = close_reason
            self.open_orders_df.at[order_index, 'profit_percentage'] = f"{profit_percentage:.2%}"
            
            logging.info(f'{execution_time}: Closed position at {current_price:.2f} with {profit_percentage:.2%} profit. Reason: {close_reason}')

class StoneWellStrategy_v2(StoneWellStrategy):
    '''
    RSI on daily timeframe. volume, death cross on hourly timeframe
    '''
    def get_indicators(self, df):
        # Calculate SMA20 of stock
        df['close_SMA'] = df['close'].rolling(window=self.price_sma_window).mean()
        df['close_short_SMA'] = df['close'].rolling(window=self.short_sma_window).mean()
        df['close_long_SMA'] = df['close'].rolling(window=self.long_sma_window).mean()

        # Calculate SMA10 and SMA20 of volume
        df['volume_short_SMA'] = df['volume'].rolling(window=self.volume_short_sma_window).mean()
        df['volume_long_SMA'] = df['volume'].rolling(window=self.volume_long_sma_window).mean()

        # Calculate Average True Range (ATR)
        df['high_low'] = df['high'] - df['low']
        df['high_close'] = abs(df['high'] - df['close'].shift())
        df['low_close'] = abs(df['low'] - df['close'].shift())
        df['true_range'] = pd.concat([df['high_low'], df['high_close'], df['low_close']], axis=1).max(axis=1)
        df['ATR'] = df['true_range'].rolling(window=self.atr_window).mean()

        # Calculate Keltner Channels
        df['KC_middle'] = df['close'].rolling(window=self.kc_sma_window).mean()
        df['KC_upper'] = df['KC_middle'] + (df['ATR'] * self.kc_mult)
        df['KC_lower'] = df['KC_middle'] - (df['ATR'] * self.kc_mult)

        # Calculate KC_position
        df['KC_position'] = (df['close'] - df['KC_lower']).clip(lower=0) / (df['KC_upper'] - df['KC_lower'])

        return df
    
    def get_extra_indicators(self, df):
        # Calculate RSI
        delta = df['close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=self.rsi_window).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=self.rsi_window).mean()
        rs = gain / loss
        df['RSI'] = 100 - (100 / (1 + rs))

        # Calculate SMA20 of RSI14
        df['RSI_SMA'] = df['RSI'].rolling(window=self.rsi_sma_window).mean()
        
        # Calculate RSI 2 
        gain_2 = (delta.where(delta > 0, 0)).rolling(window=self.rsi_window_2).mean()
        loss_2 = (-delta.where(delta < 0, 0)).rolling(window=self.rsi_window_2).mean()
        rs_2 = gain_2 / loss_2
        df['RSI_2'] = 100 - (100 / (1 + rs_2))
        
        # Calculate EMA 12 and 26
        df['EMA_12'] = df['close'].ewm(span=12, adjust=False).mean()
        df['EMA_26'] = df['close'].ewm(span=26, adjust=False).mean()
        
        return df
 
 