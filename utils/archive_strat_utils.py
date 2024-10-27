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

# ts = BuyTheDipStrategy(trade_candles_df=min_df_chart, 
                      #  indicator_candles_df=day_df_chart, 
                      #  executions_df=exec_df, 
                      #  open_orders_df=orders_df,
                      #  tlt_dollar=300,
                      #  commission_pct=0.001,
                      #  extra_indicator_candles_df=extra_day_df_chart,
                      #  # fine tuning
                      #  rsi_window=14,
                      #  ema1_span=12,
                      #  ema2_span=26,
                      #  spy_rsi_threshold=-1,
                      #  stock_rsi_threshold=75,
                      #  overnight_return_threshold=-0.02,
                      #  profit_threshold=0.05,
                      #  max_hold_hours=6)
                      
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
    templates
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
   