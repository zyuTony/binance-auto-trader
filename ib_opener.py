import os
import math
from dotenv import load_dotenv
import pandas as pd
from utils.ib_utils import *
import warnings
from ib_insync import *

warnings.filterwarnings(
    "ignore",
    message="pandas only supports SQLAlchemy connectable")

load_dotenv()
DB_USERNAME = os.getenv('RDS_USERNAME')
DB_PASSWORD = os.getenv('RDS_PASSWORD')
DB_HOST = os.getenv('RDS_ENDPOINT')
DB_NAME = 'financial_data'


ib = IB()
ib.connect('127.0.0.1', 7496, clientId=1)

# get from sql
conn = connect_to_db(DB_NAME, DB_HOST, DB_USERNAME, DB_PASSWORD)
# create_latest_trades_table(conn) # already created

query = f"""
with key_pairs as (
    select *, row_number() over (partition by symbol order by date desc) as rn
    from stock_historical_price
),
key_pairs_120d as (
    select *
    from key_pairs
    where rn <= 120
),
ols_spread as (
    select a.date, a.symbol as symbol_a, b.symbol as symbol_b,
    a.close as close_a, b.close as close_b,
    a.close - c.ols_coeff * b.close as ols_spread, c.*
    from key_pairs_120d a
    join key_pairs_120d b
    on a.date = b.date
    join stock_signal c
    on c.symbol1 = a.symbol and c.symbol2 = b.symbol
),
bb_band as (
    select *,
    coalesce(avg(ols_spread) over (partition by symbol_a, symbol_b order by date rows between 19 preceding and current row), ols_spread) as sma,--ADJUSTABLE
    coalesce(stddev(ols_spread) over (partition by symbol_a, symbol_b order by date rows between 19 preceding and current row), 0) as sd--ADJUSTABLE
    from ols_spread
),
ranked_results as (
    select
    symbol_a, symbol_b, date,
    round(close_a, 2) as close_a, round(close_b, 2) as close_b,
    round(ols_spread, 2) as ols_spread,
    round(most_recent_coint_pct, 2) as most_recent_coint_pct,
    round(recent_coint_pct, 2) as recent_coint_pct,
    round(hist_coint_pct, 2) as hist_coint_pct,
    round(r_squared, 2) as r_squared,
    round(ols_constant, 2) as ols_constant,
    round(ols_coeff, 3) as ols_coeff,
    round(((ols_spread - sma)/nullif(2 * sd, 0)) * 100, 0) as key_score,
    case
        when abs(ols_coeff) < 1 then round(close_a/abs(ols_coeff) + close_b, 2)
        else round(close_a + abs(ols_coeff)*close_b, 2)
    end as investment,
    round(abs(ols_spread - sma), 2) as potential_win,
    round(sma, 2) as rolling_mean,
    round(sma + 1.8 * sd, 2) as upper_band, round(sma - 1.8 * sd, 2) as lower_band,
    row_number() over (partition by symbol_a, symbol_b order by date desc) as rn
    from bb_band
)
select symbol_a, symbol_b, date,
most_recent_coint_pct, recent_coint_pct, hist_coint_pct,
r_squared, ols_constant, ols_coeff,
round(potential_win/nullif(investment, 0), 4) as potential_win_pct,
key_score, investment, potential_win
from ranked_results
where rn = 1 and potential_win/nullif(investment, 0) >= {IB_MIN_POTENTIAL_WIN_PCT}
and most_recent_coint_pct >= {IB_MIN_RECENT_COINT}
and r_squared >= {IB_MIN_R_SQUARED}
order by most_recent_coint_pct desc, recent_coint_pct desc,
hist_coint_pct desc, potential_win_pct desc;
"""
monitored_pairs_df = pd.read_sql(query, conn)

if os.path.exists(ib_order_csv_file):
    orders_df = pd.read_csv(ib_order_csv_file)
    if orders_df.empty:
        orders_df = pd.DataFrame()
else:
    orders_df = pd.DataFrame()

for index, row in monitored_pairs_df.iterrows():
    symbol_Y = row['symbol_a']
    symbol_X = row['symbol_b']
    ols_coeff = row['ols_coeff']
    ols_constant = row['ols_constant']
    print(
        f"---\n Checking {symbol_Y} X {symbol_X} - coeff:{round(ols_coeff,2)} constant:{round(ols_constant,2)}")

    '''Check whether pair already traded or stop loss closed'''
    already_traded = False
    for _, order_row in orders_df.iterrows():
        order_pair_set = {order_row['symbol_Y'], order_row['symbol_X']}
        if {symbol_Y, symbol_X} == order_pair_set and order_row['pair_trade_status'] in {
                "OPEN", "UPPER_STOPPED", "LOWER_STOPPED"}:
            already_traded = True
            print(
                f"ALREADY TRADED with status: {order_row['pair_trade_status']}.")
            break
    if already_traded:
        continue

    '''Monitoring for opens'''
    # 1. connect get latest real time data.
    try:
        contract_Y = Stock(symbol_Y, 'SMART', 'USD')
        Y_minute_data, Y_daily_data = get_ib_data(ib, contract_Y)
    except Exception as e:
        print(f"No Data for {symbol_Y} on Binance: {str(e)}")
        continue
    try:
        contract_X = Stock(symbol_Y, 'SMART', 'USD')
        X_minute_data, X_daily_data = get_ib_data(ib, contract_X)
    except Exception as e:
        print(f"No Data for {symbol_X} on Binance: {str(e)}")
        continue

    # in case time difference make data different length
    if len(Y_minute_data) != len(X_minute_data):
        print(
            f"Length mismatch between Y_minute_data ({len(Y_minute_data)}) and X_minute_data ({len(X_minute_data)})")
        continue

    if len(Y_daily_data) != len(X_daily_data):
        print(
            f"Length mismatch between Y_daily_data ({len(Y_daily_data)}) and X_daily_data ({len(X_daily_data)})")
        continue

    # 2. calculate spread.
    minute_spread = calculate_spread(
        Y_minute_data,
        X_minute_data,
        ols_coeff,
        ols_constant)
    daily_spread_range = calculate_bollinger_bands(
        calculate_spread(
            Y_daily_data,
            X_daily_data,
            ols_coeff,
            ols_constant),
        IB_BB_BAND_WINDOW,
        IB_BB_SIGNAL_STD_MULT,
        IB_BB_STOPLOSS_STD_MULT).drop(
            columns=['spread'])

    minute_spread['date_only'] = minute_spread['date'].dt.date
    min_spread_w_day_band = pd.merge(
        minute_spread,
        daily_spread_range,
        left_on='date_only',
        right_on=daily_spread_range['date'].dt.date,
        how='left')
    min_spread_w_day_band = min_spread_w_day_band.drop(
        columns=['date_only', 'date_y'])
    min_spread_w_day_band.rename(columns={'date_x': 'date'}, inplace=True)

    # 3. trade based on the spread. long x and short y.
    latest_min = min_spread_w_day_band.iloc[-1]
    print(
        f"current:{round(latest_min['spread'], 2)} upper:{round(latest_min['upper_band'], 2)} lower:{round(latest_min['lower_band'], 2)}")

    if latest_min['spread'] > latest_min['upper_band']:
        strat = 'short Y long X'
    elif latest_min['spread'] < latest_min['lower_band']:
        strat = 'long Y short X'
    else:
        strat = 'stand_by'

    latest_strat = pd.DataFrame([{'date': latest_min['date'],
                                  'symbol_Y': symbol_Y,
                                  'symbol_X': symbol_X,
                                  'strategy': strat}])
    if not os.path.exists(ib_strat_csv_file):
        latest_strat.to_csv(ib_strat_csv_file, mode='w', header=True, index=False)
    else:
        latest_strat.to_csv(
            ib_strat_csv_file,
            mode='a',
            header=False,
            index=False)

    '''Execute trade at prime condition'''
    if latest_strat['strategy'].iloc[-1] != 'stand_by':
        print(f"Executing a trade for {symbol_Y}X{symbol_X}...")
        symbol_Y = latest_strat['symbol_Y'].iloc[-1]
        symbol_X = latest_strat['symbol_X'].iloc[-1]

        # determine order size with formula
        curr_price_Y = Y_minute_data['close'].iloc[-1]
        curr_price_X = X_minute_data['close'].iloc[-1]

        # if single share price higher than per trade $, dont execute
        if curr_price_Y > IB_TOTAL_USDT_PER_TRADE/2:
            print(f'{symbol_Y} at ${curr_price_Y} too expensive. skip this trade')
            continue
        if curr_price_X > IB_TOTAL_USDT_PER_TRADE/2:
            print(f'{symbol_X} at ${curr_price_X} too expensive. skip this trade')
            continue
        
        amt_Y = IB_TOTAL_USDT_PER_TRADE / (curr_price_X * ols_coeff + curr_price_Y)
        amt_X = ols_coeff * amt_Y
        amt_Y = math.ceil(amt_Y)
        amt_X = math.ceil(amt_X)
        
        usdt_on_Y = amt_Y * curr_price_Y
        usdt_on_X = amt_X * curr_price_X
        
        print("curr_price_Y, amt_Y, curr_price_X, amt_X, usdt_on_Y, usdt_on_X")
        print(curr_price_Y, amt_Y, curr_price_X, amt_X, usdt_on_Y, usdt_on_X)

        if latest_strat['strategy'].iloc[-1] == 'long Y short X':
            # long Y
            long_order_info =  MarketOrder('BUY', amt_Y)
            long_order = ib.placeOrder(contract_Y, long_order_info)
            print(
                f'longed {symbol_Y}. bought {amt_Y} shares of ${amt_Y*curr_price_Y}')
            send_ib_executed_orders_to_sql(conn, long_order)

            # short X
            short_order_info = MarketOrder('SELL', amt_X)
            short_order = ib.placeOrder(contract_X, short_order_info)
            print(
                f'shorted {symbol_X}. short sold {amt_X} shares of ${amt_X*curr_price_X}')
            send_ib_executed_orders_to_sql(conn, short_order)
            orders_df = pd.concat([orders_df,
                                   ib_pairs_order_to_pd_df("OPEN",   
                                                        latest_min,
                                                        ols_coeff,
                                                        ols_constant,
                                                        long_order,
                                                        short_order,
                                                        symbol_Y,
                                                        symbol_X)])

        elif latest_strat['strategy'].iloc[-1] == 'short Y long X':
            # long X
            long_order_info =  MarketOrder('BUY', amt_X)
            long_order = ib.placeOrder(contract_X, long_order_info)
            print(
                f'longed {symbol_X}. bought {amt_X} of them of ${amt_X*curr_price_X}')
            send_ib_executed_orders_to_sql(conn, long_order)
           
            # short Y
            short_order_info = MarketOrder('SELL', amt_Y)
            short_order = ib.placeOrder(contract_Y, short_order_info)
            print(
                f'shorted {symbol_Y}. short sold {amt_Y} of them of ${amt_Y*curr_price_Y}')
            send_ib_executed_orders_to_sql(conn, short_order)
            orders_df = pd.concat([orders_df,
                                   ib_pairs_order_to_pd_df("OPEN",
                                                        latest_min,
                                                        ols_coeff,
                                                        ols_constant,
                                                        long_order,
                                                        short_order,
                                                        symbol_Y,
                                                        symbol_X)])

        orders_df.to_csv(ib_order_csv_file, index=False)
        print(f'Updated the new order to {ib_order_csv_file}!')
    else:
        print(f"NO TRADE")

conn.close()
ib.disconnect()