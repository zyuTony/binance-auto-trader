import pandas as pd
from abc import ABC, abstractmethod
from binance.client import Client
from utils.trading_utils import *
from binance.enums import *
from binance.helpers import round_step_size
import requests
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
    
    def __init__(self, trade_candles_df, indicator_candles_df, executions_df, open_orders_df, tlt_dollar, commission_pct, extra_indicator_candles_df):
        self.trade_candles_df = trade_candles_df
        self.indicator_candles_df = indicator_candles_df
        self.extra_indicator_candles_df = extra_indicator_candles_df
        self.executions_df = executions_df
        self.open_orders_df = open_orders_df
        self.tlt_dollar = tlt_dollar
        self.commission_pct = commission_pct
        
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
        else:
            return f"Unknown frequency: {time_diff}"

    def _update_open_orders_logs(self, last_update_time, status, symbol, tlt_dollar, price, quantity): 
        new_order = {
            'last_update_time': last_update_time, 
            'status': status, 
            'symbol': symbol, 
            'tlt_dollar': tlt_dollar, 
            'price': price, 
            'quantity': quantity}
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
                num_open_trades += 1
        logging.info(f'Closed all {num_open_trades} remaining open trades!')
    
    def run_test(self): 
        # check df frequencies
        trade_df_timestamp = self._check_candle_frequency(self.trade_candles_df)
        indi_df_timestamp = self._check_candle_frequency(self.indicator_candles_df)
        extra_indi_df_timestamp = self._check_candle_frequency(self.extra_indicator_candles_df)
        
        # get indicators
        self.indicator_candles_df = self.get_indicators(self.indicator_candles_df)
        self.extra_indicator_candles_df = self.get_extra_indicators(self.extra_indicator_candles_df) if self.extra_indicator_candles_df is not None else None
        
        # transform based on timeframe of the dataframes
        if trade_df_timestamp == indi_df_timestamp == extra_indi_df_timestamp:
            self.trade_candles_df = pd.merge(self.trade_candles_df, self.indicator_candles_df, on='date', how='left', suffixes=('', '_indi'))
            self.trade_candles_df = pd.merge(self.trade_candles_df, self.extra_indicator_candles_df, on='date', how='left', suffixes=('', '_exindi'))
        elif indi_df_timestamp == extra_indi_df_timestamp and trade_df_timestamp != indi_df_timestamp:
            self.trade_candles_df['date_day'] = self.trade_candles_df['date'].dt.date
            self.indicator_candles_df['date'] = pd.to_datetime(self.indicator_candles_df['date']).dt.date
            self.extra_indicator_candles_df['date'] = pd.to_datetime(self.extra_indicator_candles_df['date']).dt.date
            
            self.trade_candles_df = self.trade_candles_df.merge(self.indicator_candles_df, left_on='date_day', right_on='date', suffixes=('', '_indi'), how='left')
            self.trade_candles_df = self.trade_candles_df.merge(self.extra_indicator_candles_df, left_on='date_day', right_on='date', suffixes=('', '_exindi'), how='left')
            logging.info('All Columns in trading df: ', self.trade_candles_df.columns) 
        else:
            logging.warning("Indicator and extra indicator dataframes have different timeframes. Will not run test")
            return -1
        # go through the all trade df row by row
        # for _, candle_df_slice in tqdm(self.trade_candles_df.iterrows()): 
        for idx in tqdm(range(len(self.trade_candles_df))): 
            candle_df_slice = self.trade_candles_df.iloc[idx]
            
            # opening 
            self.stepwise_logic_open(candle_df_slice)
            # closing
            for order_index, open_order in self.open_orders_df.iterrows():
                if open_order['status'] == 'OPEN':
                    self.stepwise_logic_close(candle_df_slice, order_index)
        
        # wrap up all trades
        self.close_all_trades()        
        logging.info(f'Finished test run!')

    def trading_summary(self):
        df = self.executions_df
        if df.empty:
            print("No trades executed! No Summary")
            return None

        # Calculate profit
        df['profit'] = df.apply(lambda row: row['tlt_dollar'] if row['action'] == 'SELL' else -row['tlt_dollar'], axis=1)
        total_profit = df['profit'].sum()

        total_volume = df['tlt_dollar'].sum()
        total_commission = total_volume * self.commission_pct
        
        percent_profit = (total_profit / total_volume) * 100 if total_volume > 0 else 0
        
        # Calculate buy and hold
        trade_df = self.trade_candles_df
        first_price = trade_df.iloc[0]['close']
        last_price = trade_df.iloc[-1]['close']
        buy_and_hold_quantity = total_volume / first_price
        buy_and_hold_profit = (last_price - first_price) * buy_and_hold_quantity
        buy_and_hold_percent = (buy_and_hold_profit / total_volume) * 100
        
        # Calculate total number of trades
        total_trades = len(df)
        
        summary = {
            "Total Number of Trades": total_trades,
            "Total Profit": f"${total_profit:.2f}",
            "Total Trading Volume": f"${total_volume:.2f}",
            "Percent Profit": f"{percent_profit:.2f}%",
            "Total Commission Cost": f"${total_commission:.2f}",
            "Buy and Hold Profit": f"${buy_and_hold_profit:.2f}",
            "Buy and Hold Percent": f"{buy_and_hold_percent:.2f}%"
        }
        
        for key, value in summary.items():
            print(f"{key}: {value}")
        
        return summary
  
class BinanceProductionStrategy(Strategy):
    
    def __init__(self, bn_client, trade_candles_df, indicator_candles_df, executions_df, ideal_executions_df, open_orders_df, tlt_dollar, commission_pct, extra_indicator_candles_df):
        super().__init__(trade_candles_df, indicator_candles_df, executions_df, open_orders_df, tlt_dollar, commission_pct, extra_indicator_candles_df)
        self.bn_client = bn_client
        self.commission_pct=0
        self.ideal_executions_df = ideal_executions_df # the price we wanted to buy - to cross check with actual
    
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
        self.indicator_candles_df = self.get_indicators(self.indicator_candles_df)
        if self.extra_indicator_candles_df is not None:
            self.extra_indicator_candles_df = self.get_extra_indicators(self.extra_indicator_candles_df)
            
        if self._check_candle_frequency(self.trade_candles_df) == self._check_candle_frequency(self.indicator_candles_df):
            same_tf = True
        else:
            same_tf = False
 
        candle_df_slice = self.trade_candles_df.iloc[-1]
        # opening 
        self.stepwise_logic_open(candle_df_slice, same_tf)
        # closing
        for order_index, open_order in self.open_orders_df.iterrows():
            if open_order['status'] == 'OPEN':
                self.stepwise_logic_close(candle_df_slice, same_tf, order_index)     
        logging.info(f'Finished runinng once for latest data!')

'''kids'''
class BuyTheDipStrategy(TestStrategy):
    '''
    INDICATORS: 
    SPY - RSI; EMA1; EMA2 
    traded stock - RSI; EMA1; EMA2; overnight Return
    BUY WHEN SPY RSI < spy_rsi_threshold AND stock RSI > stock_rsi_threshold AND candle returns < overnight_return_threshold
    SELL on same day or when profit threshold is reached
    ''' 
    def __init__(self, trade_candles_df, indicator_candles_df, executions_df, open_orders_df, tlt_dollar, commission_pct, extra_indicator_candles_df,
                 rsi_window, ema1_span, ema2_span, spy_rsi_threshold, stock_rsi_threshold,
                 overnight_return_threshold, profit_threshold, max_hold_hours):
        super().__init__(trade_candles_df, indicator_candles_df, executions_df, open_orders_df, tlt_dollar, commission_pct, extra_indicator_candles_df)
        self.rsi_window = rsi_window
        self.ema1_span = ema1_span
        self.ema2_span = ema2_span
        self.spy_rsi_threshold = spy_rsi_threshold
        self.stock_rsi_threshold = stock_rsi_threshold
        self.overnight_return_threshold = overnight_return_threshold
        self.profit_threshold = profit_threshold
        self.max_hold_time = pd.Timedelta(hours=max_hold_hours)

    def get_indicators(self, df):
        # Calculate RSI
        delta = df['close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=self.rsi_window).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=self.rsi_window).mean()
        rs = gain / loss
        df['RSI'] = 100 - (100 / (1 + rs))
        
        # Calculate EMAs
        df[f'EMA{self.ema1_span}'] = df['close'].ewm(span=self.ema1_span, adjust=False).mean()
        df[f'EMA{self.ema2_span}'] = df['close'].ewm(span=self.ema2_span, adjust=False).mean()
        df['overnight_return'] = (df['open'] - df['close'].shift(1)) / df['close'].shift(1)
        return df
    
    def get_extra_indicators(self, df):
        # Calculate RSI
        delta = df['close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=self.rsi_window).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=self.rsi_window).mean()
        rs = gain / loss
        df['RSI'] = 100 - (100 / (1 + rs))
        
        # Calculate EMAs
        df[f'EMA{self.ema1_span}'] = df['close'].ewm(span=self.ema1_span, adjust=False).mean()
        df[f'EMA{self.ema2_span}'] = df['close'].ewm(span=self.ema2_span, adjust=False).mean()
        df['overnight_return'] = (df['open'] - df['close'].shift(1)) / df['close'].shift(1)
        return df
        
    def stepwise_logic_open(self, trade_candle_df_slice): 
        curr_candle = trade_candle_df_slice 
        # LOGIC 
        
        if curr_candle['RSI'] >= self.stock_rsi_threshold and curr_candle[f'EMA{self.ema1_span}']>=curr_candle[f'EMA{self.ema1_span}'] and curr_candle['overnight_return'] < self.overnight_return_threshold:
            # curr_candle[f'RSI_exindi'] <= self.spy_rsi_threshold and 
            last_update_time = execution_time = curr_candle['date']
            # Check if there's no open DKS order
            if self.open_orders_df.empty or self.open_orders_df[(self.open_orders_df['symbol'] == 'DKS') & (self.open_orders_df['status'] == 'OPEN')].empty:
                symbol = 'DKS'   
                price = curr_candle['close']
                quantity = self.tlt_dollar / price   
                 
                self.buy(self.tlt_dollar*(1+self.commission_pct), execution_time, symbol, price, quantity)
                self._update_open_orders_logs(last_update_time, 'OPEN', symbol, self.tlt_dollar, price, quantity)
                logging.info(f'{last_update_time}: Opened position at {price:.2f} with RSI {curr_candle["RSI"]:.2f} and overnight return {curr_candle["overnight_return"]:.2%}')
            else:
                logging.debug(f"{last_update_time}: DKS position already open, skipping new order")
        else:
            logging.debug(f"{curr_candle['date']}: opening stand by - extra rsi:{curr_candle['RSI_exindi']:.2f} - stock rsi:{curr_candle['RSI']:.2f} - return:{curr_candle['overnight_return']:.2%}")
          
    def stepwise_logic_close(self, trade_candle_df_slice,order_index):
        order = self.open_orders_df.iloc[order_index]
        open_price = order['price']
        open_time = pd.to_datetime(order['last_update_time'])
        
        last_candle = trade_candle_df_slice
        current_price = last_candle['close']
        current_time = pd.to_datetime(last_candle['date'])
        
        profit_percentage = (current_price - open_price) / open_price  
        time_difference = current_time - open_time 
        
        if profit_percentage >= self.profit_threshold or time_difference >= self.max_hold_time:
            execution_time = last_candle['date']
            symbol = order['symbol']
            price = last_candle['close']
            quantity = order['quantity']
            tlt_dollar = price * quantity
            self.sell(quantity, execution_time, symbol, tlt_dollar*(1-self.commission_pct), current_price)
            self.open_orders_df.at[order_index, 'status'] = 'CLOSED'
            
            if profit_percentage >= self.profit_threshold:
                logging.info(f'{execution_time}: Closed position with {profit_percentage:.2f}% profit!')
            else:
                logging.info(f'{execution_time}: Closed position after {self.max_hold_time} with {profit_percentage:.2f}% profit/loss.')

class BNDummyStrategy(BinanceProductionStrategy):
    '''
    INDICATORS: None
    always buy
    always sell
    '''
    def get_indicators(self, df):
        return df
    
    def stepwise_logic_open(self, trade_candle_df_slice, same_tf): 
        last_candle = trade_candle_df_slice 
        if same_tf:
            # use indi same as trading tf
            indicator_row = self.indicator_candles_df[self.indicator_candles_df['date'] == last_candle['date']].iloc[0] # same tf
        else:
            indicator_row = self.indicator_candles_df[self.indicator_candles_df['date'] == str(pd.to_datetime(last_candle['date']).date())].iloc[0]  

        # LOGIC 
        if True:
            symbol = 'BTCUSDT'   
            last_update_time = execution_time = last_candle['date']
            price = last_candle['close']
            quantity = self.tlt_dollar / price   
             
            executed_order = self.buy(self.tlt_dollar, execution_time, symbol, price, quantity)
            self._update_open_orders_logs(executed_order['executed_time_str'], 'OPEN', 
                                          symbol, 
                                          executed_order['executed_tlt_dollar'], 
                                          executed_order['executed_price'], 
                                          executed_order['executed_quantity'])
            logging.info(f'{last_update_time}: Opened position at {price:.2f}')
        else:
            logging.debug('not opening')
            
    def stepwise_logic_close(self, trade_candle_df_slice, same_tf, order_index):
        order = self.open_orders_df.iloc[order_index]
        open_price = order['price'] 
        last_candle = trade_candle_df_slice
        current_price = last_candle['close'] 
        
        profit_percentage = (current_price - open_price) / open_price  
        if True:
            execution_time = last_candle['date']
            symbol = order['symbol']
            price = last_candle['close']
            quantity = order['quantity']
            tlt_dollar = price * quantity
            
            executed_order = self.sell(quantity, execution_time, symbol, tlt_dollar, current_price)
            self.open_orders_df.at[order_index, 'status'] = 'CLOSED'
            logging.info(f'{execution_time}: Closed position with {profit_percentage*100:.2f}% profit!')
          
class DummyStrategy(TestStrategy):
    '''
    INDICATORS: None
    always buy
    always sell
    '''
    def get_indicators(self, df):
        return df
    
    def stepwise_logic_open(self, trade_candle_df_slice, same_tf): 
        last_candle = trade_candle_df_slice 
        if same_tf:
            # use indi same as trading tf
            indicator_row = self.indicator_candles_df[self.indicator_candles_df['date'] == last_candle['date']].iloc[0] # same tf
        else:
            # manual update if timeframe different TODO
            indicator_row = self.indicator_candles_df[self.indicator_candles_df['date'] == str(pd.to_datetime(last_candle['date']).date())].iloc[0]  

        # LOGIC 
        if True:
            symbol = 'BTCUSDT'   
            last_update_time = execution_time = last_candle['date']
            price = last_candle['close']
            quantity = self.tlt_dollar / price   
             
            executed_order = self.buy(self.tlt_dollar, execution_time, symbol, price, quantity)
            self._update_open_orders_logs(executed_order['executed_time_str'], 'OPEN', 
                                          symbol, 
                                          executed_order['executed_tlt_dollar'], 
                                          executed_order['executed_price'], 
                                          executed_order['executed_quantity'])
            logging.info(f'{last_update_time}: Opened position at {price:.2f}')
        else:
            logging.debug('not opening')
            
    def stepwise_logic_close(self, trade_candle_df_slice, same_tf, order_index):
        order = self.open_orders_df.iloc[order_index]
        open_price = order['price'] 
        last_candle = trade_candle_df_slice
        current_price = last_candle['close'] 
        
        profit_percentage = (current_price - open_price) / open_price  
        if True:
            execution_time = last_candle['date']
            symbol = order['symbol']
            price = last_candle['close']
            quantity = order['quantity']
            tlt_dollar = price * quantity
            
            executed_order = self.sell(quantity, execution_time, symbol, tlt_dollar, current_price)
            self.open_orders_df.at[order_index, 'status'] = 'CLOSED'
            logging.info(f'{execution_time}: Closed position with {profit_percentage*100:.2f}% profit!')
   