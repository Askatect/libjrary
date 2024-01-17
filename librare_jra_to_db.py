from pyjap.logger import LOG
LOG.print_to_console()

from pyjap.handlers.SQL import SQLHandler

import os

directory = "SQL"
local = SQLHandler(
    driver = '{SQL Server}',
    server = 'NUCLEUS',
    database = 'personal',
    encrypt = 'no'
)
################################################################
filenames = os.listdir(directory)
filenames = [filename for filename in filenames if filename.endswith('.sql') and filename.startswith(('ufn_', 'usp_', 'vw_', 'v_'))]
for filename in filenames:
    with open(directory + '/' + filename, 'r') as file:
        lines = file.read().splitlines()
        go = (lines[-1] == 'GO')
        local.execute_query('\n'.join(lines[0:len(lines) - int(go)]), commit = True)