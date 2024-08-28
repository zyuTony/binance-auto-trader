import pandas as pd
from abc import ABC, abstractmethod
class Strategy(ABC):
    def __init__(self, candles_df, executions_df, open_orders_df):
        required_columns = ['open', 'high', 'low', 'close']
        if not all(col in dataframe.columns for col in required_columns):
            raise ValueError("DataFrame must contain 'open', 'high', 'low', and 'close' columns")
        self.candles_df = candles_df
        self.executions_df = executions_df
        self.open_orders_df = open_orders_df

    @abstractmethod
    def buy(self): # different between test and real
        pass

    @abstractmethod
    def sell(self): # different between test and real
        pass

    @abstractmethod
    def _update_open_orders_logs(self):  
        pass

    @abstractmethod
    def _update_execution_logs(self): 
        pass

    @abstractmethod
    def stepwise_logic_open(self):  # determine whether open
        pass

    @abstractmethod
    def stepwise_logic_close(self): # determine whether close
        pass


class TestStrategy(Strategy):
    
    def _update_open_orders_logs(self, last_update_time, status, symbol, tlt_dollar, price, quantity): 
        new_order = {'last_update_time': last_update_time, 'status': status, 'symbol': symbol, 'tlt_dollar': tlt_dollar, 'price': price, 'quantity': quantity}
        self.open_orders_df.at[len(self.open_orders_df)] = new_order
  
    def _update_execution_logs(self, execution_time, symbol, tlt_dollar, price, quantity):   
        new_exec = {'execution_time': execution_time, 'symbol': symbol, 'tlt_dollar': tlt_dollar, 'price': price, 'quantity': quantity}
        self.executions_df.at[len(self.executions_df)] = new_exec
        
    def buy(self, tlt_dollar, execution_time, symbol, price, quantity): 
        self._update_execution_logs(execution_time, symbol, tlt_dollar, price, quantity)

    def sell(self, quantity, execution_time, symbol, tlt_dollar, price):  
        self._update_execution_logs(execution_time, symbol, tlt_dollar, price, quantity)
        
    def stepwise_logic_open(self): #TODO usually need different timeframes of data - min for trade; hr, day for signal
        self.candles_df['SMA20'] = self.candles_df['close'].rolling(window=20).mean()
        last_candle = self.candles_df.iloc[-1]
        if last_candle['close'] > last_candle['SMA20']:
            tlt_dollar = 1000  # Example amount, adjust as needed
            price = last_candle['close']
            quantity = tlt_dollar / price
            execution_time = last_candle.name  # Assuming the index is the timestamp
            symbol = self.candles_df['symbol'].iloc[-1]  # Assuming 'symbol' column exists
            self.buy(tlt_dollar, execution_time, symbol, price, quantity)
            print('Opened position!')
        print('Passed open logic!')

    def stepwise_logic_close(self): #TODO usually need different timeframes of data - min for trade; hr, day for signal
        if len(self.open_orders_df) == 0:
            return False
        for index, order in self.open_orders_df.iterrows():
            if order['status'] == 'OPEN':
                last_candle = self.candles_df.iloc[-1]
                if (last_candle['close'] - order['price']) / order['price'] >= 0.05:
                    execution_time = last_candle.name  # Assuming the index is the timestamp
                    symbol = order['symbol']
                    tlt_dollar = order['quantity'] * last_candle['close']
                    price = last_candle['close']
                    quantity = order['quantity']
                    self.sell(quantity, execution_time, symbol, tlt_dollar, price)
                    self.open_orders_df.at[index, 'status'] = 'CLOSED'
                    print(f'Closed order for {symbol}!')
        
        print('Passed close logic!')
 
 
  
class ProductionStrategy(Strategy):

    def buy(self): # real BN execution
        pass


    def sell(self):  # real BN execution
        pass
         