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

class Logger:
    def __init__(self, now: datetime = datetime.now(), location: str = 'logs'):
        prefix = __main__.__file__.split('.')[0].split("\\")[-1]
        self.file = f'{location}/{prefix}-log_{now.strftime("%Y%m%d%H%M%S%f")[:-3]}.txt'
        os.makedirs(os.path.dirname(self.file), exist_ok = True)

        logging.basicConfig(
            filename = self.file,
            filemode = 'w',
            format = '\n--==#### {levelno}-{levelname} {asctime} ####==--  --== {module}.{funcName}:{lineno} ==--\n{message}',
            style = '{',
            level = 'INFO'
        )

        logging.info("Logger configured!")
        return
    
    def __str__(self):
        return self.file
    
    def log_file_reader(self):
        return open(self.file, 'r')