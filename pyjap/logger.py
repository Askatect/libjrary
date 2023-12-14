import logging
import __main__
import os
from datetime import datetime

def logger(
    name: str = None, 
    location: str = 'logs',
    file: str = None, 
    now: datetime = datetime.now(),
    level_value: int|None = 20, 
    level_name: str|None = 'INFO'
):
    name = name or __name__
    if name in logging.Logger.manager.loggerDict:
        log = logging.getLogger(name)
        log.info("New module added to logger.") # Maybe add inspect to get name of added module?
        return log
    log = logging.getLogger(name)

    level_value = level_value or 20
    level_name = (level_name or 'INFO').upper()
    if f'Level {level_name}' != logging.getLevelName(level_name):
        level_value = logging.getLevelName(level_name)
    try:
        level_value = min(level_value, logging.getLevelName(level_name))
    except TypeError:
        level_value = level_value
    if f'Level {level_value}' != logging.getLevelName(level_value):
        level_name = logging.getLevelName(level_value)
    log.setLevel(level_name) # Add try...except for custom logging levels.

    if file is None:
        prefix = os.path.splitext(os.path.basename(__main__.__file__))[0]
        file = f'{location}/{prefix}-log_{now.strftime("%Y%m%d%H%M%S%f")[:-3]}.txt'
    elif '.' not in file:
        file = file + '.txt'
    os.makedirs(os.path.dirname(file), exist_ok = True)
    file_handler = logging.FileHandler(file)

    formatter = logging.Formatter(fmt = '\n--==#### {levelno}-{levelname} {asctime} ####==--  --== {module}.{funcName}:{lineno} ==--\n{message}', style = '{')
    file_handler.setFormatter(formatter)

    log.addHandler(file_handler)
    log.info(f'Logger "{name}" configured!')
    return log

LOG = logger()