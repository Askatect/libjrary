import logging
import __main__
import os
from datetime import datetime

class Logger(logging.Logger):
    def __init__(
        self, 
        name: str = None, 
        location: str = 'logs',
        file: str = None, 
        now: datetime = datetime.now(),
        level_value: int|None = 20, 
        level_name: str|None = 'INFO'
    ):
        self.name = name or __name__
        super().__init__(self.name, 0)

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
        self.level_value = level_value
        self.level_name = level_name
        self.setLevel(level_name) # Add try...except block for custom levels.

        self.formatter = logging.Formatter(
            fmt = '--==## {levelno}-{levelname} {asctime} ##==--  --== {module}.{funcName}:{lineno} ==--\n{message}\n', 
            style = '{'
        )

        if file is None:
            prefix = os.path.splitext(os.path.basename(__main__.__file__))[0]
            file = f'{location}/{prefix}-log_{now.strftime("%Y%m%d%H%M%S%f")[:-3]}.txt'
        elif '.' not in file:
            file = file + '.txt'
        self.file = file
        return
    
    def __str__(self):
        return self.file
    
    def print_to_console(self, formatter: logging.Formatter = None, strm = None):
        console_handler = logging.StreamHandler(stream = strm)
        console_handler.setFormatter(formatter or self.formatter)
        self.addHandler(console_handler)
        self.info("Console logging configured.")
        return
    
    def print_to_file(
        self, 
        filepath: str = None,
        formatter: logging.Formatter = None
    ):
        filepath = filepath or self.file
        self.info(f'File logging configured for "{filepath}".')
        os.makedirs(os.path.dirname(filepath), exist_ok = True)
        file_handler = logging.FileHandler(filepath)
        file_handler.setFormatter(formatter or self.formatter)
        self.addHandler(file_handler)
        return

LOG = Logger()