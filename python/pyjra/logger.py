"""
# logger.py

Version: 2.0
Authors: JRA
Date: 2024-03-19

#### Explanation: 
Use the LOG constant for standardised logging across pyjra.

#### Requirements:
- logging: Python's built-in logging package is used as a basis.
- os
- datetime.datetime

#### Artefacts:
- Logger (class): Child class of logging.Logger.
- LOG (pyjra.logger.Logger): The rocemmended instance of the pyjra.logger.Logger to use.

#### Usage: 
>>> from pyjra.logger import LOG
>>> LOG.print_to_console()
>>> LOG.info("Using the log.")

#### History:
- 2.0 JRA (2024-03-19): LOG v2.0.
- 1.2 JRA (2024-03-15): LOG v1.2.
- 1.1 JRA (2024-02-13): Logger v1.1.
- 1.0 JRA (2024-02-08): Initial version.
"""
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
        level: int|str = 20
    ):
        """
        ### __init__

        Version: 1.1
        Authors: JRA
        Date: 2024-03-19

        #### Explanation:
        Initialises the logger.

        #### Parameters:
        - name (str): The name of the logger. Defaults to __name__.
        - location (str): The path to store log files at. Defaults to 'logs'.
        - file (str): The filename to give to the log file. Defaults to '<__main__.__name__>_logs-<current datetime>.txt'.
        - now (datetime): The time to append to the name of the log file. Defaults to current time.
        - level (int|str): The level to assign to the logger, could be a value or a name. Defaults to level 20 ('INFO').

        #### Usage:
        >>> LOG = Logger()

        #### History:
        - 1.1 JRA (2024-03-19): Logging now depends only on the value and reformatted the standard formatter.
        - 1.0 JRA (2024-02-08): Initial version.
        """
        self.name = name or __name__
        super().__init__(self.name, 0)

        self.level = self.__level_check(level)
        self.setLevel(self.level)

        self.formatter = logging.Formatter(
            fmt = '--==## {asctime} {levelno}-{levelname} ##==--  --== {module}.{funcName}:{lineno} ==--\n{message}\n', 
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
        """
        ### __str__

        Version: 1.0
        Authors: JRA
        Date: 2024-02-08

        #### Explanation:
        Returns the default file of the logger.

        #### Returns:
        - (str)

        #### Usage:
        >>> print(LOG)

        #### Tasklist:
        - Maybe the name of the logger should be used instead?

        #### History:
        - 1.0 JRA (2024-02-08): Initial version.
        """
        return self.file
    
    def __level_check(
        self,
        level: int|str
    ) -> int:
        """
        ### __level_check

        Version: 2.0
        Authors: JRA
        Date: 2024-03-19

        #### Explanation:
        Standardises level names and values.

        #### Parameters:
        - level (int|str): The logging level to standardise.

        #### Returns:
        - level (int): The value of the logging level.

        #### Usage:
        >>> LOG.__level_check('debug')
        10

        #### History:
        - 2.0 JRA (2024-03-19): Complete rewrite to only use one variable to capture the logging level.
        - 1.0 JRA (2024-03-15): Initial version.
        """
        if isinstance(level, str):
            level = logging.getLevelName(level)
            if isinstance(level, str):
                raise ValueError(f'Logging {level.lower()} does not exist.')
        elif isinstance(level, int):
            if logging.getLevelName(level).startswith('Level '):
                raise ValueError(f'Logging {level.lower()} does not exist.')
        else:
            raise TypeError('Level identifiers must be string names or integer values.')
        return level
    
    def set_level(self, level: int|str):
        """
        ### set_level

        Version: 1.0
        Authors: JRA
        Date: 2024-03-19

        #### Explanation:
        Allows the logging level of the logger to be changed.

        #### Requirements:
        - Logger.__level_check (func)

        #### Parameters:
        - level (int|str): The logging level.

        #### Usage:
        >>> LOG.set_level(10)

        #### History:
        - 1.0 JRA (2024-03-19): Initial version.
        """
        self.level = self.__level_check(level)
        self.setLevel(self.level)
        return
    
    def print_to_console(
        self, 
        formatter: logging.Formatter = None, 
        strm = None, 
        level: int|str = None
    ):
        """
        ### print_to_console

        Version: 1.1
        Authors: JRA
        Date: 2024-03-19

        #### Explanation:
        Use this method to have the logger print to the console.

        #### Parameters:
        - formatter (logging.Formatter): The formatter to use. Defaults to the pyjra standard.
        - strm: The stream to write to. Defaults to stderr.
        - level (int|str): The logging level to print to console.

        #### Usage:
        >>> LOG.print_to_console()

        #### History:
        - 1.1 JRA (2024-03-19): Added level.
        - 1.0 JRA (2024-02-08): Initial version.
        """
        console_handler = logging.StreamHandler(stream = strm)
        console_handler.setLevel(level or self.level)
        console_handler.setFormatter(formatter or self.formatter)
        self.addHandler(console_handler)
        self.info(f"Console logging configured at level {level or self.level}.")
        return
    
    def print_to_file(
        self, 
        filepath: str = None,
        formatter: logging.Formatter = None,
        level: int|str = None
    ):
        """
        ### print_to_file

        Version: 1.1
        Authors: JRA
        Date: 2024-03-19

        #### Explanation:
        Use this method to have the logger print to a file.

        #### Parameters:
        - filepath (str): The filepath to write to. Defaults to the instance default.
        - level (int|str): The logging level to print to file.

        #### Usage:
        >>> LOG.print_to_file()

        #### History:
        - 1.1 JRA (2024-03-19): Added level.
        - 1.0 JRA (2024-02-08): Initial version.
        """
        filepath = filepath or self.file
        os.makedirs(os.path.dirname(filepath), exist_ok = True)
        file_handler = logging.FileHandler(filepath)
        file_handler.setLevel(level or self.level)
        file_handler.setFormatter(formatter or self.formatter)
        self.addHandler(file_handler)
        self.info(f"Console logging configured at level {level or self.level}.")
        return

    def add_to_root_logger(self):
        """
        ### add_to_root_logger

        Version: 1.0
        Authors: JRA
        Date: 2024-02-13

        #### Explanation:
        Adds the logger to the root logger, so log records to the logger should pass to the root logger also.

        #### Usage:
        >>> LOG.add_to_root_logger()

        #### History:
        - 1.0 JRA (2024-02-13): Initial version.
        """
        self.parent = logging.getLogger()
        return
    
    def define_logging_level(self, name: str, value: int):
        """
        ### define_logging_level

        Version: 1.0
        Authors: JRA
        Date: 2024-03-19

        #### Explanation:        
        Generate a custom logging level.

        #### Parameters:
        - name (str): The name of the logging level.
        - value (int): The value of the logging level.

        #### Usage:
        >>> LOG.define_logging_level('Verbose', 5)
        >>> LOG.verbose('Verbose message.')

        #### Tasklist:
        - Add a check to make sure the logging level does not already exist.
        
        #### History:
        - 1.0 JRA (2024-03-19): Initial version.
        """
        logging.addLevelName(value, name.upper())
        def logging_method(message):
            self.log(value, message)
            return
        setattr(self, name.lower().replace(' ', '_'), logging_method)
        self.info(f'Added logging level {name} at level {value}.')
        return

LOG = Logger()
"""
## Logger

Version: 2.0
Authors: JRA
Date: 2024-03-19

#### Explanation:
Use the LOG constant for standardised logging across pyjra.

#### Artefacts:
- name (str): The name of the logger.
- level (int): The value of the logger's logging level.
- formatter (logging.Formatter): The format of the logging level.
- file (str): The default file to direct logs to if using `print_to_file`.
- __init__ (func): Initialises the logger.
- __str__ (func): Returns the default file of the logger.
- __level_check (func): Standardises level names and values.
- set_level (func): Allows the logging level of the logger to be changed.
- print_to_console (func): Use this method to have the logger print to the console.
- print_to_file (func): Use this method to have the logger print to a file.
- add_to_root_logger (func): Adds the logger to the root logger, so log records to the logger should pass to the root logger also.
- define_logging_level (func): Generate a custom logging level.

#### Usage:
>>> LOG = Logger()
>>> LOG.print_to_file()

#### History:
- 2.0 JRA (2024-03-19): __init__ v1.1, __level_check v2.0, print_to_console v1.1, print_to_file v1.1. Added set_level and define_logging_level.
- 1.2 JRA (2024-03-15): Added __level_check.
- 1.1 JRA (2024-02-13): Added add_to_root_logger.
- 1.0 JRA (2024-02-08): Initial version.
"""
