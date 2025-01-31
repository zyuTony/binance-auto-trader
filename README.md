# Cryptocurrency Pairs Trading Bot

A statistical arbitrage trading bot that automatically identifies and trades cointegrated cryptocurrency pairs on Binance.

## Overview

This bot implements a pairs trading strategy by:

- Monitoring cryptocurrency pairs for cointegration relationships
- Opening positions when statistical arbitrage opportunities arise
- Managing risk and closing positions based on predefined criteria

## Key Components

### Trading Scripts

- `opener.py`: Entry signal monitoring and position opening

  - Queries database for cointegrated pairs
  - Calculates optimal entry points using Bollinger Bands
  - Executes paired long/short trades when conditions align

- `closer.py`: Position management and exit execution
  - Tracks all open positions
  - Monitors spread mean reversion
  - Closes positions when profit targets or stop losses hit

### Analysis Tools

- `strat_explore.py`: Strategy backtesting and analysis

  - Tests trading logic on historical data
  - Generates performance metrics and charts
  - Helps validate strategy parameters

- `strat_param_tuning.py`: Parameter optimization
  - Runs backtests across parameter combinations
  - Visualizes results for strategy refinement
  - Exports data for downstream analysis

## Setup Instructions

1. Create `.env` file with required credentials:

   - Binance API keys
   - Database connection details
   - Other configuration parameters

2. Configure automated execution:

   - Set up cron job to run `execute_trade.sh`
   - Recommended frequency: 1-5 minute intervals
   - Script handles trade execution and position management

3. Monitor performance:
   - Check trading logs for execution details
   - Review database for trade history
   - Use analysis tools to evaluate strategy
