"""
# logger.py

Version: 1.0
Authors: JRA
Date: 2024-02-08

#### Explanation: 
Use the LOG constant for standardised logging across pyjap.

#### Requirements:
- logging: Python's built-in logging package is used as a basis.
- os
- datetime.datetime

#### Artefacts:
- Logger (class): Child class of logging.Logger.
- LOG (pyjap.logger.Logger): The rocemmended instance of the pyjap.logger.Logger to use.

#### Usage: 
>>> from pyjap.logger import LOG
>>> LOG.print_to_console()
>>> LOG.info("Using the log.")

#### History:
- 1.0 JRA (2024-02-08): Initial version.
"""
import logging
import __main__
import os
from datetime import datetime

class Logger(logging.Logger):
    """
    ## Logger

    Version: 1.0
    Authors: JRA
    Date: 2024-02-08

    #### Explanation:
    Use the LOG constant for standardised logging across pyjap.

    #### Artefacts:
    - name (str): The name of the logger.
    - level_name (str): The name of the logger's logging level.
    - level_value (int): The value of the logger's logging level.
    - formatter (logging.Formatter): The format of the logging level.
    - file (str): The default file to direct logs to if using `print_to_file`.
    - __init__ (func): Initialises the logger.
    - __str__ (func): Returns the default file of the logger.
    - print_to_console (func): Use this method to have the logger print to the console.
    - print_to_file (file): Use this method to have the logger print to a file.

    #### Usage:
    >>> LOG = Logger()
    >>> LOG.print_to_file()

    #### Tasklist:
    - Add a method for LOG to use the root logger (see "Install .whl from GitHub" conversation with ChatGPT).
    - Add facility for logging levels.

    #### History: 
    - 1.0 JRA (2024-02-08): Initial version.
    """
    def __init__(
        self, 
        name: str = None, 
        location: str = 'logs',
        file: str = None, 
        now: datetime = datetime.now(),
        level_value: int|None = 20, 
        level_name: str|None = 'INFO'
    ):
        """
        ### __init__

        Version: 1.0
        Authors: RJA
        Ddate: 2024-08-02

        #### Explanation:
        Initialises the logger.

        #### Parameters:
        - name (str): The name of the logger. Defaults to __name__.
        - location (str): The path to store log files at. Defaults to 'logs'.
        - file (str): The filename to give to the log file. Defaults to '<__main__.__name__>_logs-<current datetime>.txt'.
        - now (datetime): The time to append to the name of the log file. Defaults to current time.
        - level_value (int): The value of the logger's logging level.
        - level_name (str): The name of the logger's logging level.

        #### Usage:
        >>> LOG = Logger()

        #### History:
        - 1.0 JRA (2024-02-08): Initial version.
        """
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
    
    def print_to_console(self, formatter: logging.Formatter = None, strm = None):
        """
        ### print_to_console

        Version: 1.0
        Authors: JRA
        Date: 2024-02-08

        #### Explanation:
        Use this method to have the logger print to the console.

        #### Parameters:
        - formatter (logging.Formatter): The formatter to use. Defaults to the pyjap standard.
        - strm: The stream to write to. Defaults to stderr.

        #### Usage:
        >>> LOG.print_to_console()

        #### History:
        - 1.0 JRA (2024-02-08): Initial version.
        """
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
        """
        ### print_to_file

        Version: 1.0
        Authors: JRA
        Date: 2024-02-08

        #### Explanation:
        Use this method to have the logger print to a file.

        #### Parameters:
        - filepath (str): The filepath to write to. Defaults to the instance default.

        #### Usage:
        >>> LOG.print_to_file()

        #### History:
        - 1.0 JRA (2024-02-08): Initial version.
        """
        filepath = filepath or self.file
        self.info(f'File logging configured for "{filepath}".')
        os.makedirs(os.path.dirname(filepath), exist_ok = True)
        file_handler = logging.FileHandler(filepath)
        file_handler.setFormatter(formatter or self.formatter)
        self.addHandler(file_handler)
        return

LOG = Logger()
LOG.__doc__ = Logger.__doc__