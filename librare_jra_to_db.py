from pyjap.logger import LOG
# LOG.print_to_console()
LOG.print_to_file()

from pyjap.sql import SQLHandler

import os
import pyodbc

directory = "C:/Users/JoshAppleton/OneDrive - Euler DataOps & Analytics Ltd/Documents/Guide Dogs/Integrations/SQL"
local = SQLHandler(environment='gdba_test'
    # driver = '{SQL Server}',
    # server = 'NUCLEUS',
    # database = 'personal',
    # encrypt = 'no'
)
################################################################
filenames = os.listdir(directory)
filenames = [filename for filename in filenames if filename.endswith('.sql') and filename.startswith(('ufn_', 'usp_', 'vw_', 'v_'))]
print('\n'.join(filenames))
if input(f"Load the above files to {local}? (Y/n)\n") == "Y":
    for filename in filenames:
        LOG.info(f"Loading file {filename}...")
        try:
            with open(directory + '/' + filename, 'r') as file:
                lines = file.read().splitlines()
                go = (lines[-1] == 'GO')
                local.execute_query('\n'.join(lines[0:len(lines) - int(go)]), commit = True)
        except UnicodeDecodeError as error:
            msg = f'Could not load {filename}. {error}'
            LOG.error(msg)
            print(msg)
        except pyodbc.ProgrammingError as error:
            msg = f'Error creating {filename}. {error}'
            LOG.error(msg)
            print(msg)
        else:
            LOG.info(f"Loaded file {filename}.")
else:
    print("Cancelled: no files have been loaded.")