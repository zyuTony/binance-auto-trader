#!/bin/bash

current_date=$(date +"%Y-%m-%d")

# # Log and run opener.py
# echo -e "--------------------------------------\n---- $(date) ----" >> /home/ec2-user/opener.log 2>&1
# /home/ec2-user/binance_pair_trader/venv/bin/python3 /home/ec2-user/binance_pair_trader/opener.py >> /home/ec2-user/opener.log 2>&1

# # Log and run closer.py
# echo -e "--------------------------------------\n---- $(date) ----" >> /home/ec2-user/closer.log 2>&1
# /home/ec2-user/binance_pair_trader/venv/bin/python3 /home/ec2-user/binance_pair_trader/closer.py >> /home/ec2-user/closer.log 2>&1

# Log and run opener.py
echo -e "--------------------------------------\n---- $(date) ----" >> /home/ec2-user/logs/opener_$current_date.log 2>&1
/home/ec2-user/binance_pair_trader/venv/bin/python3 /home/ec2-user/binance_pair_trader/opener.py >> /home/ec2-user/logs/opener_$current_date.log 2>&1

# Log and run closer.py
echo -e "--------------------------------------\n---- $(date) ----" >> /home/ec2-user/logs/closer_$current_date.log 2>&1
/home/ec2-user/binance_pair_trader/venv/bin/python3 /home/ec2-user/binance_pair_trader/closer.py >> /home/ec2-user/logs/closer_$current_date.log 2>&1