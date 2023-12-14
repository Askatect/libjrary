import os
import logging
from datetime import datetime
import __main__

class Logger:
    def __init__(
        self, 
        logger_name: str = None, 
        now: datetime = datetime.now(), 
        location: str = 'logs', 
        file: str = None, 
        level_value: int|None = 20,
        level_name: str|None = 'INFO'
    ):
        self.logger_name = logger_name or __name__
        
        if file is None:
            prefix = os.path.splitext(os.path.basename(__main__.__file__))[0]
            self.file = f'{location}/{prefix}-log_{now.strftime("%Y%m%d%H%M%S%f")[:-3]}.txt'
        elif '.' not in file:
            self.file = file + '.txt'
        else:
            self.file = file

        self.level_value = level_value or 20
        self.level_name = (level_name or 'INFO').upper()

        level_value = logging.getLevelName(self.level_name)
        if level_value != f'Level {self.level_name}':
            self.level_value = level_value

        try:
            self.level_value = min(self.level_value, logging.getLevelName(self.level_name))
        except TypeError:
            self.level_value = self.level_value

        level_name = logging.getLevelName(self.level_value)
        if level_name != f'Level {self.level_value}':
            self.level_name = level_name

        if self.logger_name in logging.Logger.manager.loggerDict:
            self.logger = logging.getLogger(self.logger_name)
            self.logger.info(f'Module added to logger "{self.logger_name}".')
            return
        
        os.makedirs(os.path.dirname(self.file), exist_ok = True)

        self.logger = logging.getLogger(self.logger_name)
        try:
            self.logger.setLevel(self.level_name)
        except ValueError:
            self.add_level(level_value, level_name)
        formatter = logging.Formatter(fmt = '\n--==#### {levelno}-{levelname} {asctime} ####==--  --== {module}.{funcName}:{lineno} ==--\n{message}', style = '{')
        file_handler = logging.FileHandler(self.file)
        file_handler.setFormatter(formatter)
        self.logger.addHandler(file_handler)
        self.logger.info(f'Logger "{self.logger_name}" configured!')
        return

    def __str__(self):
        return self.logger_name
    
    def add_level(self, level_value: int, level_name: str):
        level_name = level_name.upper()
        if hasattr(logging, level_name.lower()):
            raise AttributeError(f'There is already a definition for {level_name}.')
        if hasattr(self, level_name.lower()):
            raise AttributeError(f'Existing logger already has a definition for {level_name} .')
        
        def log_at_level(self, message, *args, **kwargs):
            if self.logger.isEnabledFor(level_value):
                self.logger._log(level_value, message, *args, **kwargs)

        logging.addLevelName(level_value, level_name)
        setattr(logging, level_name, level_value)
        setattr(self, level_name.lower(), log_at_level)
        print('HERE')
        return
    
LOG = Logger(level_name = 'INF', level_value = 15)
print(LOG.level_name)
print(LOG.level_value)
LOG.inf('Stuff')