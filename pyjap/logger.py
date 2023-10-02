from datetime import datetime
import logging
import os
import __main__

# Existing logging levels:
# 50 - CRITICAL
# 40 - ERROR
# 30 - WARNING
# 20 - INFO
# 10 - DEBUG
#  0 - NOTSET

if 'now' not in globals():
    now = datetime.now()

prefix = __main__.__file__.split('.')[0].split("\\")[-1]
file = f'logs/{prefix}-log_{now.strftime("%Y%m%d%H%M%S%f")[:-3]}.txt'
os.makedirs(os.path.dirname(file), exist_ok = True)

logging.basicConfig(
    filename = file,
    filemode = 'w',
    format = '{asctime} {levelname} {message}',
    style = '{',
    level = 'INFO'
)

logging.info('Logger configured.')