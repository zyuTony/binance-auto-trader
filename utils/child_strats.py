from utils.strat_utils import *

import pandas as pd
from binance.enums import *
import logging

'''kids'''
class SimpleSMAStrategy(TestStrategy):
    '''
    INDICATORS: 
    Price SMA
    BUY -> 
    1) close >= SMA
    '''

    def __init__(self, *args, price_sma_window, **kwargs):
        super().__init__(*args, **kwargs)
        self.price_sma_window = price_sma_window

    def get_indicators(self, df):
        # Calculate SMA20 of stock
        df['price_SMA'] = df['close'].rolling(window=self.price_sma_window).mean()
        return df
    
    def get_extra_indicators(self, df):
        return df
        
    def stepwise_logic_open(self, trade_candle_df_slices): 
        curr_candle = trade_candle_df_slices.iloc[-1]
        prev_candle = trade_candle_df_slices.iloc[-2]
        
        """LOGIC"""
        open_conditions = [
            (curr_candle['open'] > curr_candle['price_SMA'], "Price above SMA"),
        ]
        """LOGIC ENDS"""
        
        # Check if all conditions are met
        open_reason = None
        for condition, reason in open_conditions:
            if condition:
                open_reason = reason
                break

        if open_reason:
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
                logging.info(f'{last_update_time}: Opened position at {price:.2f}. Reason: {open_reason}')
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
        
        """LOGIC"""
        close_reason = ""
        close_conditions = [
            (profit_percentage <= self.stoploss_threshold, f"Stop loss hit (P%: {profit_percentage:.1%})"),
            (profit_percentage >= self.profit_threshold, f"Profit target met (P%: {profit_percentage:.1%})"),
            (current_price <= high_since_open * (1 - self.max_high_retrace) and profit_percentage > 0, f"Price retraced but profitable"),
            (curr_candle['open'] < curr_candle['price_SMA'], f"Price < SMA"),
        ]
        """LOGIC ENDS"""
        
        for condition, reason in close_conditions:
            if condition:
                close_reason = reason
                break
            
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


class StoneWellStrategy(TestStrategy):
    '''
    INDICATORS: 
    SMA20 of stock; RSI14; SMA20 of RSI14; SMA10/20 of volume 
    BUY -> 
    1) close >= SMA20D 
    2) RSI14D > SMA20D(RSI14D) 
    3) SMA50D <= SMA200D 
    4) SMA10D(vol) > SMA20D(vol)
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
            # and curr_candle['KC_position'] <= 0.5
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
 
 