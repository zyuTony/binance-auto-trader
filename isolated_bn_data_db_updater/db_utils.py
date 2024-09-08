from abc import ABC, abstractmethod
import os
import json
import pandas as pd
import psycopg2
from psycopg2 import OperationalError
from psycopg2.extras import execute_values
import logging
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)8s | %(message)s',
    datefmt='%Y-%m-%d %H:%M' #datefmt='%Y-%m-%d %H:%M:%S'
)

def convert_to_float(value):
    if value is None or value == "" or value == "None" or value == "-":
        return None
    try:
        return float(value)
    except (ValueError, TypeError) as e:
        logging.error(f"Error converting to float: {e}")
        return None

def convert_to_int(value):
    if value is None or value == "" or value == "None" or value == "-":
        return None
    try:
        return int(value)
    except (ValueError, TypeError) as e:
        logging.error(f"Error converting to int: {e}")
        return None
    
def convert_to_date(value):
    if value in ("None", '-'):
        return None
    try:
        return datetime.strptime(value, '%Y-%m-%d').date()
    except ValueError as e:
        logging.error(f"Error converting to date: {e}")
        return None

def convert_to_datetime(date_str):
    try:
        return datetime.fromisoformat(date_str.replace("Z", ""))
    except ValueError as e:
        logging.error(f"Error converting to datetime: {e}")
        return None
    
def truncate_string(value, length):
    try:
        if value and len(value) > length:
            logging.warning(f'Truncating {value} at {length}')
        return value[:length] if value and len(value) > length else value
    except Exception as e:
        logging.error(f"Error truncating string: {e}")
        return None


class db_refresher(ABC): 
    '''object that 1) connect to db 2) transform and insert json data depends on source.
       template for coin_gecko_db and avan_stock_db'''
    def __init__(self, db_name, db_host, db_username, db_password, table_name):
        self.db_name = db_name
        self.db_host = db_host
        self.db_username = db_username
        self.db_password = db_password
        self.conn = None
        self.table_name = table_name  
        self.table_creation_script = None  # This will be set in child classes
        self.data_insertion_script = None  # This will be set in child classes
         
    def connect(self):
        try:
            self.conn = psycopg2.connect(
                host=self.db_host,
                database=self.db_name,
                user=self.db_username,
                password=self.db_password)
            print(f"Connected to {self.db_host} {self.db_name}!")
        except OperationalError as e:
            print(f"Error connecting to database: {e}")
            self.conn = None

    def close(self):
        if self.conn:
            self.conn.close()
            print("Database connection closed.")
      
    def create_table(self):
        cursor = self.conn.cursor()
        try:
            cursor.execute(self.table_creation_script)
            self.conn.commit()
            logging.info(f"{self.table_name} created successfully.")
        except Exception as e:
            logging.error(f"Failed to create table: {str(e)}")
            self.conn.rollback()
        finally:
            cursor.close()
    
    @abstractmethod
    def _data_transformation(self, file_path):
        pass
    
    def insert_data(self, file_path):
        time_series_data = self._data_transformation(file_path)
        cursor = self.conn.cursor()
        try:
            execute_values(cursor, self.data_insertion_script, time_series_data)
            self.conn.commit()
            logging.debug(f"Inserted into {self.table_name} from {file_path}")
        except Exception as e:
            logging.error(f"Failed to insert data from {file_path}: {e}")
            self.conn.rollback()
        finally:
            cursor.close()

class coin_gecko_OHLC_db_refresher(db_refresher):
    '''handle all data insertion from OHLC data via coin gecko api'''
    def __init__(self, *args):
        super().__init__(*args)
        
        self.table_creation_script = f"""
        CREATE TABLE IF NOT EXISTS {self.table_name} (
            symbol VARCHAR(20) NOT NULL,
            date TIMESTAMPTZ NOT NULL,
            open NUMERIC NOT NULL,
            high NUMERIC NOT NULL,
            low NUMERIC NOT NULL,
            close NUMERIC NOT NULL,
            PRIMARY KEY (symbol, date)
        );
        """
        
        self.data_insertion_script = f"""
        INSERT INTO {self.table_name} (symbol, date, open, high, low, close)
        VALUES %s
        ON CONFLICT (symbol, date)
        DO UPDATE SET
            open = EXCLUDED.open,
            high = EXCLUDED.high,
            low = EXCLUDED.low,
            close = EXCLUDED.close
        WHERE {self.table_name}.open <> EXCLUDED.open
            OR {self.table_name}.high <> EXCLUDED.high
            OR {self.table_name}.low <> EXCLUDED.low
            OR {self.table_name}.close <> EXCLUDED.close;
        """
        
    def _data_transformation(self, file_path):
        try:
            with open(file_path, 'r') as file:
                data = json.load(file)
            symbol = os.path.splitext(os.path.basename(file_path))[0]
            outputs = []
            seen_dates = set()
            
            for entry in data:
                date = pd.to_datetime(entry[0], unit='ms').strftime('%Y-%m-%d')
                open_price  = entry[1]
                high = entry[2]
                low = entry[3]
                close = entry[4]
                               
                if date not in seen_dates:# this is to deal with gc duplicated data that cause errors sometimes
                    outputs.append([symbol, date, open_price, high, low, close])
                    seen_dates.add(date)  
            return outputs     
        except Exception as e:
            logging.debug(f"Data transformation failed for {symbol}: {e}")
            return None

class binance_OHLC_db_refresher(db_refresher):
    '''handle all data insertion from OHLC data via coin gecko api'''
    def __init__(self, *args):
        super().__init__(*args)
        
        self.table_creation_script = f"""
        CREATE TABLE IF NOT EXISTS {self.table_name} (
            symbol VARCHAR(20) NOT NULL,
            date TIMESTAMPTZ NOT NULL,
            open NUMERIC NOT NULL,
            high NUMERIC NOT NULL,
            low NUMERIC NOT NULL,
            close NUMERIC NOT NULL,
            volume NUMERIC NOT NULL,
            PRIMARY KEY (symbol, date)
        );
        """
        
        self.data_insertion_script = f"""
        INSERT INTO {self.table_name} (symbol, date, open, high, low, close, volume)
        VALUES %s
        ON CONFLICT (symbol, date)
        DO UPDATE SET
            open = EXCLUDED.open,
            high = EXCLUDED.high,
            low = EXCLUDED.low,
            close = EXCLUDED.close,
            volume = EXCLUDED.volume
        WHERE {self.table_name}.open <> EXCLUDED.open
            OR {self.table_name}.high <> EXCLUDED.high
            OR {self.table_name}.low <> EXCLUDED.low
            OR {self.table_name}.close <> EXCLUDED.close
            OR {self.table_name}.volume <> EXCLUDED.volume;
        """
        
    def _data_transformation(self, file_path):
        try:
            with open(file_path, 'r') as file:
                data = json.load(file)
            symbol = os.path.splitext(os.path.basename(file_path))[0]
            outputs = []
            seen_dates = set()
            
            for entry in data:
                date = pd.to_datetime(entry[0], unit='ms').strftime('%Y-%m-%d %H:%M')
                open_price  = entry[1]
                high = entry[2]
                low = entry[3]
                close = entry[4]
                volume = entry[5]
                               
                if date not in seen_dates:# this is to deal with gc duplicated data that cause errors sometimes
                    outputs.append([symbol, date, open_price, high, low, close, volume])
                    seen_dates.add(date)  
            return outputs     
        except Exception as e:
            logging.debug(f"Data transformation failed for {symbol}: {e}")
            return None
