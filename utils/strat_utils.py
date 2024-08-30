import pandas as pd
from abc import ABC, abstractmethod
from binance.client import Client
from utils.trading_utils import *
from binance.enums import *
from binance.helpers import round_step_size
import random

class Strategy(ABC):
    
    def __init__(self, trade_candles_df, indicator_candles_df, executions_df, open_orders_df, tlt_dollar=100):
        self.trade_candles_df = trade_candles_df
        self.indicator_candles_df = indicator_candles_df
        self.executions_df = executions_df
        self.open_orders_df = open_orders_df
        self.tlt_dollar = tlt_dollar
    
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
    def buy(self, tlt_dollar, execution_time, symbol, price, quantity):  # different between test and real
        pass

    @abstractmethod
    def sell(self, quantity, execution_time, symbol, tlt_dollar, price):   # different between test and real
        pass

class TestStrategy(Strategy):
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.commission_pct = 0.001  # Default commission percentage of 0.1%

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
        print(f'Closed all {num_open_trades} remaining open trades!')
           
    def run_test(self): 
        self.indicator_candles_df = self.get_indicators(self.indicator_candles_df)
        
        if self._check_candle_frequency(self.trade_candles_df) == self._check_candle_frequency(self.indicator_candles_df):
            same_tf = True
        else:
            same_tf = False
        
        # go through the all trade df row by row
        for _, candle_df_slice in self.trade_candles_df.iterrows(): 
            # opening 
            self.stepwise_logic_open(candle_df_slice, same_tf)
            # closing
            for order_index, open_order in self.open_orders_df.iterrows():
                if open_order['status'] == 'OPEN':
                    self.stepwise_logic_close(candle_df_slice, same_tf, order_index)
        
        # wrap up all trades
        self.close_all_trades()        
        print(f'Finished test run!')

    def trading_summary(self):
        df = self.executions_df
        # Calculate total profit
        df['profit'] = df.apply(lambda row: row['tlt_dollar'] if row['action'] == 'SELL' else -row['tlt_dollar'], axis=1)
        total_profit = df['profit'].sum()
        
        # Count number of buy and sell trades
        num_buy = df[df['action'] == 'BUY'].shape[0]
        num_sell = df[df['action'] == 'SELL'].shape[0]
        
        # Calculate total trading volume
        total_volume = df['tlt_dollar'].sum()
        
        # Prepare summary
        summary = {
            "Total Profit": f"${total_profit:.2f}",
            "Number of Buy Trades": num_buy,
            "Number of Sell Trades": num_sell,
            "Total Trading Volume": f"${total_volume:.2f}"
        }
        for key, value in summary.items():
            print(f"{key}: {value}")
            
        return summary

    
class BinanceProductionStrategy(Strategy):
    
    def __init__(self, bn_client, trade_candles_df, indicator_candles_df, executions_df, ideal_executions_df, open_orders_df, tlt_dollar):
        super().__init__(trade_candles_df, indicator_candles_df, executions_df, open_orders_df, tlt_dollar)
        self.bn_client = bn_client
        self.ideal_executions_df = ideal_executions_df # the price we want to buy
    
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
            print(f"Error executing buy order: {str(e)}")
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
        
        print(f'longed {symbol}. bought {executed_quantity} of them of ${executed_tlt_dollar}!')
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
            print(f"Error executing sell order: {str(e)}")
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
        
        print(f'Sold {symbol}, {quantity} of them.')
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
        
        print(f'Sold ALL {symbol}, {balance_amt} of them.')
    
    def run_once(self): 
        self.indicator_candles_df = self.get_indicators(self.indicator_candles_df)
        
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
        print(f'Finished runinng once for latest data!')

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
            print(f'{last_update_time}: Opened position at {price:.2f}')
        else:
            print('not opening')
            
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
            print(f'{execution_time}: Closed position with {profit_percentage*100:.2f}% profit!')
          

        
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
            print(f'{last_update_time}: Opened position at {price:.2f}')
        else:
            print('not opening')
            
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
            print(f'{execution_time}: Closed position with {profit_percentage*100:.2f}% profit!')
          

class SimpleTestStrategy(TestStrategy):
    '''
    INDICATORS: 14 RSI; 12 EMA; 26 EMA; Candle Return
    BUY WHEN RSI > 80 AND candle returns
    SELL WHEN after 6 hours OR profit reach 0.05%
    '''
    def get_indicators(self, df):
        
        # Calculate RSI with a period of 14
        delta = df['close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
        rs = gain / loss
        df['RSI14'] = 100 - (100 / (1 + rs))
        
        # Calculate EMA 12 and EMA 16
        df['EMA12'] = df['close'].ewm(span=12, adjust=False).mean()
        df['EMA16'] = df['close'].ewm(span=16, adjust=False).mean()
        
        df['candle_return'] = (df['close'] - df['open']) / df['close'] 
        
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
        if indicator_row['RSI14'] > 75 and indicator_row['candle_return'] < -0.001:
            last_update_time = execution_time = last_candle['date']
            symbol = 'BTCUSDT'   
            price = last_candle['close']
            quantity = self.tlt_dollar / price   
             
            self.buy(self.tlt_dollar*(1+self.comission_pct), execution_time, symbol, price, quantity)
            self._update_open_orders_logs(last_update_time, 'OPEN', symbol, self.tlt_dollar, price, quantity)
            print(f'{last_update_time}: Opened position at {price:.2f} with RSI {indicator_row["RSI14"]:.2f} and day return {indicator_row["candle_return"]*100:.2%}')
          
    def stepwise_logic_close(self, trade_candle_df_slice, same_tf, order_index):
        order = self.open_orders_df.iloc[order_index]
        open_price = order['price']
        open_time = pd.to_datetime(order['last_update_time'])
        
        last_candle = trade_candle_df_slice
        current_price = last_candle['close']
        current_time = pd.to_datetime(last_candle['date'])
        
        profit_percentage = (current_price - open_price) / open_price  
        time_difference = current_time - open_time
 
        if profit_percentage >= 0.05 or time_difference >= pd.Timedelta(hours=24):
            execution_time = last_candle['date']
            symbol = order['symbol']
            price = last_candle['close']
            quantity = order['quantity']
            tlt_dollar = price * quantity
            self.sell(quantity, execution_time, symbol, tlt_dollar*(1-self.comission_pct), current_price)
            self.open_orders_df.at[order_index, 'status'] = 'CLOSED'
            
            if profit_percentage >= 0.05:
                print(f'{execution_time}: Closed position with {profit_percentage*100:.2f}% profit!')
            else:
                print(f'{execution_time}: Closed position after 6 hours with {profit_percentage*100:.2f}% profit/loss.')
        
              
