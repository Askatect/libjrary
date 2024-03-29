v: 0.1.1
Date: 2024-02-09 21:09:07

Explanation:
The libjrary features several generic tools for Python and SQL that I find useful.

Requirements:
- azure-core==1.30.0
- azure-functions==1.18.0
- azure-storage-blob==12.19.0
- certifi==2024.2.2
- cffi==1.16.0
- charset-normalizer==3.3.2
- cryptography==42.0.2
- dnspython==2.5.0
- email-validator==2.1.0.post1
- idna==3.6
- importlib-metadata==7.0.1
- isodate==0.6.1
- jaraco.classes==3.3.0
- keyring==24.3.0
- more-itertools==10.2.0
- numpy==1.26.3
- pandas==2.2.0
- phonenumbers==8.13.29
- pip==24.0
- postcode-validator==0.0.4
- pycparser==2.21
- pyodbc==5.0.1
- python-dateutil==2.8.2
- pytz==2024.1
- pywin32-ctypes==0.2.2
- requests==2.31.0
- setuptools==65.5.0
- six==1.16.0
- typing_extensions==4.9.0
- tzdata==2023.4
- urllib3==2.2.0
- wheel==0.42.0
- zipp==3.17.0

Attributes:
- azureblobstore.py
- coordinategeometry.py
- emailer.py
- formatting.py
- logger.py
- sql.py
- utilities.py
- __init__.py

Usage:
>>> pip install <path>/pyjap-0.1.1-py3-none-any.whl
or
>>> powershell -File <path>\librare_pyjap_to_wheel.ps1

History:
- 0.1.1 JRA (2024-02-09 21:09:07): Removed test code from wheel.
- 0.1.0 JRA (2024-02-09 19:58:47): Lots more documentation and retry_wait for waking paused databases.
- 0.0.7 JRA (2024-02-04): A new fix for Azure Automation.
- 0.0.6 JRA (2024-02-04): More adjustments to distribution to appease Azure Automation.
- 0.0.5 JRA (2024-02-04): Attempting new build to bugfix "more than one distribution versions".
- 0.0.4 JRA (2024-02-04): Added README automation to setup.py and added stream option to logger.py.
- 0.0.3 JRA (2024-02-04): Further edits to logger for Azure Automation.
- 0.0.2 JRA (2024-02-03): Edits to logger to improve compatibility with Azure Automation.
- 0.0.1 JRA (2024-02-03): First wheel.

