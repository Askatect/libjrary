"""
# eulib.py

Version: 1.2.3
Authors: JRA
Date: 2024-02-19

#### Explanation:
Library of functions and classes that might be useful for Euler DataOps and Analytics.

#### Requirements:
- pyjra.logger: Handles logging of processes.
- pyjra.utilities.Tabular: Table class.
- pyjra.email: Email handler.
- pyjra.sql: SQL DB handler.
- pyjra.azureblobstore: Azure blob storage handler.
- pandas: For dataframes.
- uuid: For generating unique identifiers.
- re: Regular expressions.
- datetime.datetime

#### Artefacts:
- validate_string (func): Validates a given string against a given regular expression, and can also verify a single datetime within the string using named regular expression groups.
- validate_email (func): Validates an email.
- validate_postcode (func): Validates UK postcodes.
- validate_phone (func): Validates phone numbers.
- standardiser (func): Applies validate_email, validate_postcode and validate_phone to a given dataframe.
- MDWJob (class): Handles jobs in an MDW.
- mdw_basic_query_builder (func): Provided with a source and a list of links, a query is written - with a legend to the aliases - to retrieve live data from hubs, satellites and links.

#### Usage:
>>> import eulib

#### Tasklist: 
- Is standardiser necessary?

#### History:
- 1.2.3 JRA (2024-02-22): MDWJob v2.3.
- 1.2.2 JRA (2024-02-19): More Tabular implementation for MDWJob.
- 1.2.1 JRA (2024-02-16): In the MDWJob class, began Tabular implementation and fixed a bug in __init__.
- 1.2.0 JRA (2024-02-13): Revamped error handling.
- 1.1.1 JRA (2024-02-08): Fixed a bug with tabulating data for the body_alt_text in send_job_notification in MDWJob.
- 1.1.0 JRA (2024-02-02): Replaced tabulate.tabulate with pyjra.formatting.tabulate. Added mdw_basic_query_builder.
- 1.0.0 JRA (2024-01-30): Initial version.
"""
from pyjra.logger import LOG

from pyjra.utilities import Tabular
from pyjra.sql import SQLHandler
from pyjra.emailer import EmailHandler
from pyjra.azureblobstore import AzureBlobHandler

import pandas as pd
import uuid
import re as regex
from datetime import datetime

def validate_string(string: str, target: str) -> tuple[str, None|str]:
    """
    ### validate_string

    Version: 1.0
    Authors: JRA
    Date: 2024-01-30

    #### Explanation: 
    Validates a given string against a given regular expression, and can also verify a single datetime within the string using named regular expression groups. The name of the groups should be SQL string datetime format.

    #### Requirements:
    - re: Regular expressions package.
    - datetime.datetime: For validating dates.

    #### Parameters:
    - string (str): The string to validate.
    - target (str): The regular expression to match against.

    #### Returns:
    - (str): The original string that was validated.
    - (None|str): The description of any errors that arise during validation, or None if the string was matched perfectly.

    #### Usage:
    >>> validate_string(
            string = "19991003-1234567.Csv",
            target = r"(?P<yyyy>\d{4})(?P<MM>\d{2})(?P<dd>\d{2})-(\d{7})(\.)Csv")
        )
    ("19991003-1234567.Csv", None)

    #### Tasklist:
    - The original string is not transformed, so maybe only error or None needs to be returned?
    - Add more formats to datetime_formats dictionary.

    #### History:
    - 1.0 JRA (2024-01-30): Initial version.
    """
    if not regex.match(target, string):
        return (string, 'String did not match target string.')
    target_datetime = regex.match(target, string).groupdict()
    if len(target_datetime) == 0:
        return (string, None)
    datetime_formats = {
        'yyyy': '%Y',
        'yy': '%y',
        'MM': '%m',
        'M': '%-m',
        'dd': '%d',
        'd': '%-d',
        'HH': '%H',
        'H': '%-H',
        'hh': '%I',
        'h': '%-I',
        'mm': '%M',
        'ss': '%S',
        'tt': '%p',
    }
    target_datetime = {datetime_formats[k]:v for k,v in target_datetime.items()}
    try:
        datetime.strptime("".join(target_datetime.values()), "".join(target_datetime.keys()))
    except ValueError as e:
        return (string, str(e))
    else:
        return (string, None)

class MDWJob:
    """
    ## MDWJob

    Version: 2.3
    Authors: JRA
    Date: 2024-02-22

    #### Explanation:
    Handles jobs in an MDW.

    #### Artefacts:
    - MDWJobError (class): Custom exception class for MDWJob. Has the same behaviour as the base `Exception` class. Should be used to raise errors that occur from trying to start or end jobs and supjobs.
    - MDWSQLError (class): Custom exception class for MDWJob. Has the same behaviour as the base `Exception` class. Should be used to raise errors that occur from executed SQL (other than those from the SQLHandler).
    - mdw (pyjra.sql.SQLHandler): Instance of SQL handler class that is where the job will be executed.
    - supjob_name (str): The name of the supjob being executed.
    - supjob_id (str): The identifier of the supjob being executed.
    - job_name (str): The name of the job currently being executed.
    - job_id (str): The identifier of the job currently being executed.
    - source (str): The source of the currently running job.
    - aspect (str): The aspect of the currently running job.
    - __init__ (func): Initialises the MDWJob, creates a supjob identifier and logs the start of the job.
    - __str__ (func): String representation of the supjob, including name, identifier and target MDW.
    - __del__ (func): Deletes the MDWJob and ensures that the end of the supjob has been logged.
    - __validate_job_name (func): Ensures that a job name is valid. Should be the format "<source>_<code>_<aspect>", where the code is three digits.
    - supjob_start (func): Logs the start of the supjob.
    - supjob_end (func): Logs the end of the supjob.
    - job_start (func): Attempts to start a job. It will fail if a job is still running or the given name is not in the correct format.
    - job_end (func): Ends the currently running job.
    - ingest_csv_blobs_to_mdw (func): Ingest CSV files from Azure blob storage to the MDW. Archives the files if successful and rejects them if they have a bad filename or structure.
    - __stage_blob (func): Inserts a CSV file into the "stg" schema of the MDW.
    - __structure_compliance (func): Executes the structure compliance stored procedure on a staged table.
    - __add_artificial_key_file_id (func): Executes the stored procedure to add [artificialkey], [FileId] and [id] columns to a staged table.
    - __staging_extraction (func): Calls a stored procedure that extracts data in a staging table into new staging tables based on their semantic types, and then ingests these into the MDW.
    - cleaning_transforms (func): Executes the cleaning stored procedure (without rebuild of clean satellites).
    - reset_azure_container (func): Given an Azure environment, container and folder, files one subfolder further in are moved up a directory. Useful for readying archived or rejected files for processing.
    - purge_by_recordsource (func): Executes the stored procedure to purge the MDW by record source. Has flags to purge the MDW, the staging schema and the archive schema separately.
    - send_job_notification (func): Ends the job and collects log information from the MDW to send as an email.

    #### Returns:
    - (eulib.MDWJob)

    #### Usage:
    It is not required to explicitly start and end subjobs - the class will handle this for public methods.
    >>> source_control = MDWJob(
            "source_000_aspect",
            SQL(environment = "dev")
        )
    >>> source_control.purge_by_recordsource(
            "source_001_aspect"
        )
    >>> del source_control

    Tasklist:
    - Remove dependency on pandas DataFrame in __stage_blob by using Tabular.

    #### History:
    - 2.3 JRA (2024-02-22): __stage_blob v2.0.
    - 2.2 JRA (2024-02-19): More Tabular implementation.
    - 2.1 JRA (2024-02-16): Began Tabular implementation and fixed a bug in __init__.
    - 2.0 JRA (2024-02-13): Revamped error handling.
    - 1.1 JRA (2024-02-08): Fixed a bug with tabulating data for the body_alt_text in send_job_notification.
    - 1.0 JRA (2024-01-30): "Initial" version (there have been so many developments, but this is when I'm writing the docstrings).
    """
    class MDWJobError(Exception):
        """
        ## MDWJobError

        Version: 1.0
        Authors: JRA
        Date: 2024-02-12

        #### Explanation:
        Custom exception class for MDWJob. Has the same behaviour as the base `Exception` class. Should be used to raise errors that occur from trying to start or end jobs and supjobs.

        #### Usage:
        >>> raise self.MDWJobError("Error message.")

        #### History: 
        - 1.0 JRA (2024-02-12): Initial version.
        """
        pass

    class MDWSQLError(Exception):
        """
        ## MDWJobError

        Version: 1.0
        Authors: JRA
        Date: 2024-02-12

        #### Explanation:
        Custom exception class for MDWJob. Has the same behaviour as the base `Exception` class. Should be used to raise errors that occur from executed SQL (other than those from the SQLHandler).

        #### Usage:
        >>> raise MDWJob.MDWSQLError("Error message.")

        #### History: 
        - 1.0 JRA (2024-02-12): Initial version.
        """
        pass

    def __init__(self, supjob_name: str, mdw: SQLHandler):
        """
        ### __init__

        Version: 1.2
        Authors: JRA
        Date: 2024-02-16

        #### Explanation:
        Initialises the MDWJob, creates a supjob identifier and logs the start of the job.

        #### Requirements:
        - MDWJob.supjob_start

        #### Parameters:
        - supjob_name (str): The name of the supjob.
        - mdw (pyjra.sql.SQLHandler): The target MDW to execute the supjob.

        #### Usage:
        >>> MDWJob("source_000_aspect", pyjra.sql.SQLHandler(environment = "dev"))

        #### History:
        - 1.2 JRA (2024-02-16): Fixed a bug where self.supjob_name was accessed before assignment.
        - 1.1 JRA (2024-02-13): The instance supjob name and id is now set by supjob_start.
        - 1.0 JRA (2024-01-30): Initial version.
        """
        self.mdw = mdw
        self.supjob_name = None
        self.supjob_id = None
        self.job_name = None
        self.job_id = None
        self.supjob_start(supjob_name)
        return
    
    def __str__(self):
        """
        ### __str__

        Version: 1.0
        Authors: JRA
        Date: 2024-01-30

        #### Explanation:
        String representation of the supjob, including name, identifier and target MDW.

        #### Returns:
        - (str): String representation of the supjob.

        #### Usage:
        >>> str(source_control)
        '"source_000_control" (00000000-0000-0000-0000-000000000000) on [server].[database]'

        #### History:
        - 1.0 JRA (2024-01-30)
        """
        return f'"{self.supjob_name}" ({self.supjob_id}) on {self.mdw}'
    
    def __del__(self):
        """
        ### __del__

        Version: 1.0
        Authors: JRA
        Date: 2024-01-30

        #### Explanation:
        Deletes the MDWJob and ensures that the end of the supjob has been logged.

        #### Requirements:
        - MDWJob.supjob_end

        #### Usage:
        >>> del source_control

        #### History:
        - 1.0 JRA (2024-01-30): Initial version.
        """
        if self.supjob_name is not None:
            self.supjob_end()
        return
    
    def __validate_job_name(self, job_name: str) -> tuple[str, None|str]:
        """
        ### __validate_job_name

        Version: 1.0
        Authors: JRA
        Date: 2024-01-30

        #### Explanation: 
        Ensures that a job name is valid. Should be the format "<source>_<code>_<aspect>", where the code is three digits.

        #### Parameters:
        - job_name (str): The job name to validate.

        #### Returns:
        - (str): Input job name.
        - (None|str): Validation error or None if valid.
       
        #### History: 
        - 1.0 JRA (2024-01-30): Initial version.
        """
        try:
            source, _, aspect = regex.match(r"^([a-zA-Z]+)_(\d{3})_([a-zA-Z]+)$", job_name).groups()
        except AttributeError:
            return (job_name, f'The job name "{job_name}" is invalid.')
        else:
            self.source = source
            self.aspect = aspect
            return (job_name, None)
    
    def supjob_start(self, supjob_name: str):
        """
        ### supjob_start

        Version: 2.0
        Authors: JRA
        Date: 2024-02-13

        #### Explanation:
        Logs the start of the supjob.

        #### Requirements:
        - [log].[usp_job_insert]

        #### Usage:
        >>> source_control.supjob_start()

        #### History:
        - 2.0 JRA (2024-02-13): Errors are not passed silently, and the supjob name must be passed for the supjob to start. The supjob name is also validated now.
        - 1.0 JRA (2024-01-30): Initial version.
        """
        if self.supjob_name is not None:
            error = f"Supjob {self.supjob_name} is already running!"
            LOG.error(error)
            raise MDWJob.MDWJobError(error)
        
        supjob_name, error = self.__validate_job_name(supjob_name)
        if error is not None:
            LOG.error(error)
            raise MDWJob.MDWJobError(error)
        
        self.supjob_id = str(uuid.uuid4())
        self.supjob_name = supjob_name
        LOG.info("Logging start of supjob.")
        self.mdw.execute_query("EXECUTE [log].[usp_job_insert] @jobname = ?, @jobid = ?", values = (self.supjob_name, self.supjob_id))
        return

    def supjob_end(self):
        """
        ### supjob_end

        Version: 1.1
        Authors: JRA
        Date: 2024-02-13

        #### Explanation: 
        Logs the end of the supjob.

        #### Requirements:
        - [log].[usp_job_update]

        #### Usage:
        >>> source_control.supjobstart()

        #### History:
        - 1.1 JRA (2024-02-13): Errors are no longer passed silently.
        - 1.0 JRA (2024-01-30): Initial version.
        """
        if self.supjob_name is None:
            error = "There is no active supjob to end."
            LOG.error(error)
            raise MDWJob.MDWJobError(error)
        LOG.info(f'Finished "{self.supjob_name}".')
        self.mdw.execute_query("EXECUTE [log].[usp_job_update] @jobid = ?, @status = 'Finished'", values = (self.supjob_id))
        self.supjob_name = None
        self.supjob_id = None
        return

    def job_start(self, job_name: str, filename: str = None) -> bool:
        """
        ### job_start

        Version: 2.0
        Authors: JRA
        Date: 2024-02-13

        #### Explanation:
        Attempts to start a job. It will fail if a job is still running or the given name is not in the correct format.

        #### Requirements:
        - MDWJob.__validate_job_name
        - [log].[usp_jobdetail_insert]

        #### Parameters: 
        - job_name (str): The name of the job to start.
        - filename (str): The filename associated with the job. Defaults to no filename.

        #### Returns:
        - (bool): True if job was started successfully.

        #### Usage:
        >>> source_control("source_010_aspect")

        #### History:
        - 2.0 JRA (2024-02-13): Errors are no longer silently ignored and running jobs are forcefully closed if a new one is called.
        - 1.1 JRA (2024-02-12): Error handling moved to SQLHandler class.
        - 1.0 JRA (2024-01-30): Initial version.
        """
        if self.supjob_name is None:
            error = "Supjob has not started."
            LOG.error(error)
            raise MDWJob.MDWJobError(error)
        
        job_name, error = self.__validate_job_name(job_name)
        if error is not None:
            LOG.error(error)
            raise MDWJob.MDWJobError(error)
        
        if self.job_name is not None:
            LOG.warning(f"Forcefully ending {self.job_name} to start {job_name}.")
            self.job_end()
        
        self.job_id = str(uuid.uuid4())
        self.job_name = job_name        
        self.mdw.execute_query("EXECUTE [log].[usp_jobdetail_insert] @jobname = ?, @jobdetailid = ?, @jobid = ?, @status = 'Processing', @file_id = ?", values = (self.job_name, self.job_id, self.supjob_id, filename))
        LOG.info(f'Subjob "{self.job_name}" started.')
        return
        
    def job_end(self, status: str, error_message: str = None):
        """
        ### job_end

        Version: 1.0
        Authors: JRA
        Date: 2024-01-30

        #### Explanation: 
        Ends the currently running job.

        #### Requirements:
        - [log].[usp_log_error]
        - [log].[usp_jobdetail_update]

        #### Parameters:
        - status (str): Status to assign the job.
        - error_message (str): Log an error with the job. Defaults to no error.

        #### Usage:
        >>> source_control.job_end("Complete", None)

        #### History:
        - 1.1 JRA (2024-02-13): Errors are no longer passed silently and there is a check that a job exists.
        - 1.0 JRA (2024-01-30): Initial version.
        """
        if self.job_name is None:
            error = f"There is no active job to end!"
            LOG.error(error)
            raise MDWJob.MDWJobError(error)
        
        if error_message is not None:
            LOG.warning(error_message)
            self.mdw.execute_query("EXECUTE [log].[usp_log_error] @job_id = ?, @id = ?, @message = ?", values = (self.job_id, self.supjob_id, error_message))
        else:
            LOG.info(f'Finished job "{self.job_name}" successfully.')
        
        self.mdw.execute_query("EXECUTE [log].[usp_jobdetail_update] @jobdetailid = ?, @status = ?", values = (self.job_id, status))
        self.job_id = None
        self.job_name = None
        return

    def ingest_csv_blobs_to_mdw(
        self,
        job_name: str, 
        azure_storage: AzureBlobHandler, 
        container: str, 
        filename_formats: str|list[str],
        reject_bad_filename: bool = True
    ):
        """
        ### ingest_csv_blobs_to_mdw

        Version: 2.0
        Authors: JRA
        Date: 2024-02-13

        #### Explanation:
        Ingest CSV files from Azure blob storage to the MDW. Archives the files if successful and rejects them if they have a bad filename or structure.

        #### Requirements:
        - MDWJob.job_start
        - MDWJob.job_end
        - eulib.validate_string
        - MDWJob.__stage_blob
        - MDWJob.__structure_compliance
        - MDWJob.__add_artificial_key_file_id
        - MDWJob.__staging_extraction

        #### Parameters:
        - job_name (str): Name of the ingestion job.
        - azure_storage (str): Azure blob storage hosting the desired CSV files.
        - container (str): Container of azure blob storage with the desired CSV files.
        - filename_formats (str|list[str]): Regular expression(s) that the filenames should match.
        - reject_bad_filename (bool): If true, filenames are rejected if they do not match the target expression. Useful if a folder can contain multiple valid filenames. Defaults to true.

        #### Usage:
        >>> source_control.ingest_csv_blobs_to_mdw(
                job_name = "source_010_aspect",
                azure_storage = pyjra.azureblobstore.AzureBlobHandler(environment = "dev"),
                container = "storage",
                filename_format = r"(?P<yyyy>\d{4})(?P<MM>\d{2})(?P<dd>\d{2})-(\d{7})(\.)Csv",
                reject_bad_filename = False
            )

        #### History:
        - 2.0 JRA (2024-02-13): Revamped error handling.
        - 1.2 JRA (2024-01-02): Fixed an issue where the arguments for __add_artificial_key_file_id were misaligned.
        - 1.1 JRA (2024-01-31): Added functionality for multiple acceptable filename formats.
        - 1.0 JRA (2024-01-30): Initial version.
        """
        directory = self.supjob_name[:(self.supjob_name.find("_"))] + '/'
        filename_formats = [filename_formats] if isinstance(filename_formats, str) else filename_formats

        LOG.info(f'Retrieving blob names from "{container}" at {azure_storage}.')
        filelist = azure_storage.get_blob_names(container)
        filelist = [blob for blob in filelist if blob.startswith(directory) and blob.count('/') == 1]

        for filename in filelist:
            self.job_start(job_name, filename)
        
            LOG.info("Verifying filename...")
            for filename_format in filename_formats:
                _, error = validate_string(filename, directory + filename_format)
                if error is None:
                    break
            if error is not None:
                if reject_bad_filename:
                    azure_storage.rename_blob(container, filename, filename.replace(directory, directory + 'rejected/'))
                self.job_end('Rejected', f'Filename "{filename.replace(directory, "")}" is not in an acceptable format. Allowed format{"s" if len(filename_formats) > 1 else ""}: ' + '\n{}'.format('\n'.join(filename_formats)))
                continue
            else:
                LOG.info(f'Filename "{filename}" is in a correct format.')
            
            LOG.info("Staging file...")
            try:
                self.__stage_blob(
                    azure_storage = azure_storage, 
                    container = container, 
                    filename = filename
                )
            except Exception as e:
                self.job_end('Failure', f'Unexpected {type(e)} error occurred during staging of "{filename}". {e}')
                raise
            else:
                LOG.info(f'File "{filename}" has been staged in {self.mdw}.')

            LOG.info("Performing structure compliance...")
            try:
                structure_compliance_result = self.__structure_compliance()
            except Exception as e:
                self.job_end('Failure', f'Unexpected {type(e)} error occurred during structure compliance. {e}')
                raise
            if structure_compliance_result is not None:
                azure_storage.rename_blob(container, filename, filename.replace(directory, directory + 'rejected/'))
                self.job_end('Rejected', f'File did not pass structure compliance. {structure_compliance_result}')
                continue
            else:
                del structure_compliance_result
                LOG.info("No issues found during structure compliance.")

            LOG.info("Adding [FileId], [artificialkey] and [identity] columns to the staged table...")
            try:
                self.__add_artificial_key_file_id(filename = filename)
            except Exception as e:
                self.job_end('Failure', f'Unexpected {type(e)} error occurred during addition of key columns to the staged table. {e}')
                raise
            else:
                LOG.info("Key columns added to staged table.")

            LOG.info("Performing staging extraction...")
            try:
                self.__staging_extraction()
            except Exception as e:
                self.job_end('Failure', f'Unexpected {type(e)} error occurred whilst performing staging extraction. {error}')
                raise
            else:
                LOG.info("Staging extraction complete.")

            azure_storage.rename_blob(container, filename, filename.replace(directory, directory + 'archive/'))
            self.job_end('Complete')
        return

    def __stage_blob(
        self, 
        azure_storage: AzureBlobHandler,
        container: str,
        filename: str
    ):
        """
        ### __stage_blob

        Version: 2.0
        Authors: JRA
        Date: 2024-02-22

        #### Explanation:
        Inserts a CSV file into the "stg" schema of the MDW.

        #### Requirements:
        - MDWJob.start_job

        #### Parameters:
        - azure_storage (pyjra.azureblobstore.AzureBlobs)
        - container (str)
        - filename (str)

        #### History:
        - 2.0 JRA (2024-02-22): Refactored to use Tabular.
        - 1.2 JRA (2024-02-19): Made compatible with SQLHandler v3.0.
        - 1.1 JRA (2024-02-13): Job is not started at the top of the method. Removed job_id and job_name parameters.
        - 1.0 JRA (2024-01-30): Initial version.
        """
        table = self.job_name + '_' + self.job_id

        LOG.info(f'Retrieving file "{filename}" from "{container}" at {azure_storage}...')
        data = azure_storage.get_blob_as_string(
            container = container,
            filename = filename,
            encoding = 'latin1'
        )
        data = Tabular(
            data = data,
            row_separator = '\n',
            col_separator = ',',
            header = True,
            name = filename
        )
        LOG.info('Retrieved file "{}" with columns:\n{}'.format(filename, '\n'.join(data.columns)))

        LOG.info("Ensuring uniqueness of columns...")
        for c in range(data.col_count):
            column = data.columns[c]
            if data.columns.count(column.lower()) > 1:
                column += str(data.columns[0:c + 1].count(column.lower()))
                LOG.info(f"Renaming column: {data.columns[c]} -> {column}.")
                data.columns[c] = column

        # df = azure_storage.get_blob_csv_as_dataframe(container, filename, encoding = 'latin1', header = None).map(lambda x: None if pd.isnull(x) else str(x))

        # df.rename(columns = df.iloc[0], inplace = True)
        # df.drop(df.index[0], inplace = True)
        # cols = [('\n' + str(1 + i) + ': ' + col) for i, col in enumerate(df.columns)]
        # LOG.info(f'Retrieved file "{filename}" with columns: {"".join(cols)}.')

        # name_map = {col.lower(): [] for col in df.columns if [col.lower() for col in df.columns].count(col.lower()) > 1}
        # if len(name_map) > 0:
        #     for col in df.columns:
        #         col_lower = col.lower()
        #         if col_lower in name_map.keys():
        #             name_map[col_lower].append(col + '_' + str(1 + len(name_map[col_lower])))
        #     cols = [('\n' + col_lower + ' -> ' + str(cols)) for col_lower, cols in name_map.items()]
        #     LOG.info(f'Renaming duplicate columns: {"".join(cols)}.')
        #     df = df.rename(columns = (lambda col: name_map[col.lower()].pop(0) if col.lower() in name_map.keys() else col))
        #     del col_lower
        # del name_map
        # del cols

        LOG.info(f'Staging file "{filename}" in the MDW.')
        self.mdw.insert('stg', table, data = data, replace_table = True)
        # del df
        return
    
    def __structure_compliance(self) -> None|str:
        """
        ### __structure_compliance

        Version: 1.1
        Authors: JRA
        Date: 2024-02-13

        #### Explanation: 
        Executes the structure compliance stored procedure on a staged table.

        #### Requirements:
        - MDWJob.job_start
        - [integration].[usp_structure_compliance]

        #### Returns:
        - (None|str): Details of error are returned, or None if structure is compliant.

        #### History:
        - 1.2 JRA (2024-02-19): Implemented Tabular.
        - 1.1 JRA (2024-02-13): Job is not started at the top of the method. Removed job_id and job_name parameters.
        - 1.0 JRA (2024-01-30): Initial version.
        """
        return self.mdw.execute_query("EXECUTE [integration].[usp_structure_compliance] @job = ?, @jobid = ?, @schema = ?, @error = '', @print = 0, @display = 1", values = (self.job_name, self.job_id, 'stg')).to_dict(0)[0]
    
    def __add_artificial_key_file_id(self, filename: str = None):
        """
        ### __add_artificial_key_file_id

        Version: 1.1
        Authors: JRA
        Date: 2024-02-13

        #### Explanation: 
        Executes the stored procedure to add [artificialkey], [FileId] and [id] columns to a staged table.
        
        #### Requirements:
        - MDWJob.job_start
        - [integration].[usp_add_artificial_key_file_id]

        #### Parameters:
        - filename (str)

        #### History:
        - 1.1 JRA (2024-02-13): Job is not started at the top of the method. Removed job_id and job_name parameters.
        - 1.0 JRA (2024-01-30): Initial version.
        """
        self.mdw.execute_query("EXECUTE [integration].[usp_add_artificial_key_file_id] @job_name = ?, @job_id = ?, @filename = ?, @print = 0", values = (self.job_name, self.job_id, filename or 'NULL'))
        return
    
    def __staging_extraction(self):
        """
        ### __staging_extraction

        Version: 1.1
        Authors: JRA
        Date: 2024-02-13

        #### Explanation: 
        Calls a stored procedure that extracts data in a staging table into new staging tables based on their semantic types, and then ingests these into the MDW.

        #### Requirements:
        - MDWJob.job_start
        - [integration].[usp_staging_extraction]

        #### History:
        - 1.1 JRA (2024-02-13): Job is not started at the top of the method. Removed job_id and job_name parameters.
        - 1.0 JRA (2024-01-30): Inital version.
        """
        self.mdw.execute_query("EXECUTE [integration].[usp_staging_extraction] @job = ?, @jobid = ?, @supjobid = ?, @schema = ?, @print = 0", values = (self.job_name, self.job_id, self.supjob_id, 'stg'))
        return
    
    def cleaning_transforms(self, job_name: str):
        """
        ### cleaning_transforms

        Version: 1.0
        Authors: JRA
        Date: 2024-01-30

        #### Explanation:
        Executes the cleaning stored procedure (without rebuild of clean satellites).

        #### Requirements:
        - MDWJob.job_start
        - MDWJob.job_end
        - [integration].[usp_cleaning]

        #### Parameters:
        - job_name (str): The name of the cleaning job.

        #### Usage:
        >>> source_control.cleaning_transforms("source_020_cleaning")

        #### History:
        - 2.1 JRA (2024-02-19): Implemented Tabular.
        - 2.0 JRA (2024-02-13): Revamped error handling.
        - 1.0 JRA (2024-01-30): Initial version.
        """
        self.job_start(job_name)
        try:
            LOG.info(f'Running "{self.job_name}" cleaning transformations on {self.mdw}...')
            cleaning_result = self.mdw.execute_query(
                """
DECLARE @error nvarchar(max)
EXECUTE [integration].[usp_cleaning] @jobname = ?, @jobid = ?, @error = @error OUTPUT
SELECT @error AS [error_detail]
                """, 
                values = (self.job_name, self.job_id)
            ).to_dict(0)[0]
        except Exception as e:
            self.job_end('Failure', f'Unexpected Python {type(e)} error occurred during cleaning transformations. {e}')
            raise
        else:
            if cleaning_result != None:
                error = f'SQL error occurred during cleaning transformations. {cleaning_result}'
                self.job_end('Failure', error)
                raise MDWJob.MDWSQLError(error)
            else:
                LOG.info("Cleaning transformations ran successfully.")
                self.job_end('Complete')
        return
    
#     def _update_contact_matching(self, job_name: str):
#         """
#         Update contact matching table.

#         #### Parameters:
#         - job_name (str): The name of the job.

#         Prerequisites:
#         - [integration].[contact_matching] table in SQL.
#         - [sat].[woodscontact] table in SQL.
#         """
#         if not self.job_start(job_name):
#             return
#         self.mdw.execute_query(
#             f"""
# INSERT INTO [integration].[contact_matching] ([source_hashkey])
# SELECT [hashkey]
# FROM [sat].[woodscontact]
# WHERE [loadenddate] IS NULL
#     AND [hashkey] NOT IN (SELECT [source_hashkey] FROM [integration].[contact_matching])
#             """
#         )
#         return
    
#     def _retrieve_contact_matching_dataset(self, job_name: str) -> pd.DataFrame:
#         """
#         Retrieve the contact matching dataset.

#         #### Parameters:
#         - job_name (str): The name of the job.

#         #### Returns:
#         A DataFrame containing the contact matching dataset.

#         Prerequisites:
#         - [integration].[vw_woodsnoncampaignspecificdailyexport_clean] view in SQL.
#         - [integration].[contact_matching] table in SQL.
#         - [integration].[vw_contact_matching_target] view in SQL.
#         """
#         if not self.job_start(job_name):
#             return
#         query = """
# WITH [matching_source] AS (
#     SELECT [contacthashkey],
#         [Title] AS [title],
#         [forename] AS [forename],
#         [surname] AS [surname],
#         [DOB] AS [date_of_birth],
#         [Gender] AS [sex],
#         [Address 1] AS [street],
#         NULLIF(CONCAT_WS(', ', [Address 2], [Address 3], [Address 4]), '') AS [address],
#         [Town] AS [city],
#         [County] AS [county],
#         [Postcode] AS [postcode],
#         [Country] AS [country],
#         COALESCE([Tel Num], [Mobile_Phone_number]) AS [phone],
#         [email] AS [email]
#     FROM [integration].[vw_woodsnoncampaignspecificdailyexport_clean] AS [a]
#         INNER JOIN [integration].[contact_matching] AS [b]
#             ON [b].[source_hashkey] = [a].[contacthashkey]
#             AND [b].[matched] IS NULL
#         """
#         if self.supjob_id != '00000000-0000-0000-0000-000000000000':
#             query += f"WHERE [a].[jobid] IN (SELECT [id] FROM [log].[jobdetail] WHERE [jobid] = '{self.supjob_id}')"
#         query += f"""
# )
# SELECT 'source' AS [origin], * FROM [matching_source]
# UNION
# SELECT 'target' AS [origin], 
#     [target].[contacthashkey],
#     [target].[title],
#     [target].[forename],
#     [target].[surname],
#     [target].[date_of_birth],
#     [target].[sex],
#     [target].[street],
#     [target].[address],
#     [target].[city],
#     [target].[county],
#     [target].[postcode],
#     [target].[country],
#     [target].[phone],
#     [target].[email]
# FROM [integration].[vw_contact_matching_target] AS [target]
# WHERE [target].[postcode] IN (SELECT [postcode] FROM [matching_source])
#         """
#         df = self.mdw.select_to_dataframe(query)
#         return standardiser(df)
    
    def reset_azure_container(
        self,
        job_name: str,
        azure_storage: AzureBlobHandler, 
        container: str, 
        folder: str,
        files: str|list[str] = None
    ):
        """
        ### reset_azure_container

        Version: 1.0
        Authors: JRA
        Date: 2024-01-30

        #### Explanation:
        Given an Azure environment, container and folder, files one subfolder further in are moved up a directory. Useful for readying archived or rejected files for processing.

        #### Requirements:
        - MDWJob.job_start
        - MDWJob.job_end
        
        #### Parameters:
        - job_name (str): The name of the reset job.
        - azure_storage (pyjra.azureblobstore.AzureBlobHandler): The Azure storage client.
        - container (str): The name of the container to work with in Azure storage.
        - folder (str): The name of the folder within the container to work with in Azure storage.
        - files (str|list[str]): The file(s) to be reset from one level below the given folder to that given folder. Defaults to all possible files.

        #### Usage: 
        >>> source_control.reset_azure_container(
                job_name = "source_002_azurereset",
                azure_storage = pyjra.azureblobstore.AzureBlobHandler(environment = "dev"),
                container = "storage",
                folder = "supfolder",
                files = "supfolder/subfolder/sample.txt"
            )

        #### History: 
        - 1.0 JRA (2024-01-30): Initial version.
        """
        LOG.info(f"Resetting container {container} on {azure_storage}...")
        filenames = azure_storage.get_blob_names(container)
        filenames = [filename for filename in filenames if filename.startswith(folder) and filename.count("/") > 1]
        if files is not None:
            files = [files] if isinstance(files, str) else files
            filenames = [filename for filename in filenames if filename in files]
        if len(filenames) == 0:
            LOG.warning("No files found to reset.")
        self.job_start(job_name, '; '.join(filenames) if len(filenames) > 0 else None)
        LOG.info("Resetting the following files:\n{}".format('; '.join(filenames)))
        for filename in filenames:
            azure_storage.rename_blob(container, filename, folder + "/" + filename.split("/")[-1])
        self.job_end('Complete')
        return
    
    def purge_by_recordsource(
        self, 
        job_name: str, 
        purge_mdw: bool,
        purge_stg: bool,
        purge_arc: bool
    ):
        """
        ### purge_by_recordsource

        Version: 1.0
        Authors: JRA
        Date: 2024-01-30

        #### Explanation: 
        Executes the stored procedure to purge the MDW by record source. Has flags to purge the MDW, the staging schema and the archive schema separately.

        #### Requirements:
        - MDWJob.job_start
        - MDWJob.job_end
        - [utl].[usp_purge_by_recordsource]

        #### Parameters:
        - job_name (str): Name of the purging job.
        - purge_mdw (bool): If true, hubs, links and satellites are purged.
        - purge_stg (bool): If true, the staging schema 'stg' is purged.
        - purge_arc (bool): If true, the archive schema 'arc' is purged.

        #### Usage:
        >>> source_control.purge_by_recordsource(
                job_name = "source_001_purge",
                purge_mdw = False,
                purge_stg = True,
                purge_arc = False
            )
        
        #### History:
        - 1.0 JRA (2024-01-30): Initial version.
        """
        self.job_start(job_name)
        self.mdw.execute_query(
            f"EXECUTE [utl].[usp_purge_by_recordsource] @source = ?, @aspect = ?, @purge_mdw = ?, @purge_stg = ?, @purge_arc = ?, @commit = 1, @display = 0", 
            values = (
                self.source, 
                self.aspect, 
                int(purge_mdw), 
                int(purge_stg), 
                int(purge_arc)
            )
        )
        self.job_end('Complete')
        return
    
    def send_job_notification(self, recipients: str|list[str], log_file: str = None):
        """
        ### send_job_notification

        Version: 1.4
        Authors: JRA
        Date: 2024-02-19

        #### Explanation:
        Ends the job and collects log information from the MDW to send as an email.

        #### Requirements:
        - MDWJob.supjob_end
        - [log].[jobdetail]
        - [log].[joberror]
        - [log].[routing]

        #### Parameters:
        - recipients (str|list[str]): Email address(es) to receive the job report.
        - log_file (str): Location of log file to attach to report. Optional and defaults to None.

        #### Usage:
        >>> source_control.send_job_notification("info@euler.net", str(LOG))
        
        #### History:
        - 1.4 JRA (2024-02-19): Implemented Tabular.
        - 1.3 JRA (2024-02-16): Began implementation of pyjra.utilities.Tabular.
        - 1.2 JRA (2024-02-08): Fixed a bug with tabulating data for the body_alt_text.
        - 1.1 JRA (2024-02-01): Fixed a bug where supjob name is nullified before subject header is written.
        - 1.0 JRA (2024-01-30): Initial version.
        """
        LOG.info(f'Ending and retrieving MDW log information for job {self}.')
        subject = f'Job Report for {self.__str__()}'
        job_report_query = f"""
SELECT [jd].[description] AS [Name],
    [jd].[id] AS [Subjob ID],
    CONVERT(char(23), [jd].[startdate], 21) AS [Subjob Start],
    CONVERT(char(23), [jd].[enddate], 21) AS [Subjob End],
    [jd].[status] AS [Subjob Status],
    [jd].[file_id] AS [Filename],
    [e].[message] AS [Subjob Error (if applicable)],
    STUFF((
        SELECT CONCAT(' ', [task], ': Inserted ', [inserted], ', Deactivated ', [deactivated], ', Reactivated ', [reactivated], ', Archived ', [archived], '; ', ISNULL([error], 'No errors.'))
        FROM [log].[routing] AS [r]
        WHERE [r].[job_id] = [jd].[id]
        FOR XML PATH('')
    ), 1, 1, '') AS [Routing Outcome (if applicable)]
FROM [log].[jobdetail] AS [jd]
    LEFT JOIN [log].[joberror] AS [e]
        ON [e].[job_id] = [jd].[id]
WHERE [jd].[jobid] = '{self.supjob_id}'
ORDER BY [jd].[startdate] ASC
            """
        self.supjob_end()
        job_report_data = self.mdw.execute_query(query = job_report_query, name = subject)
        LOG.info("Writing log information to HTML.")
        body_html = job_report_data.to_html(
            colours = {
                'main': '#380435', 
                'black': '#092318', 
                'white': '#ffffff', 
                'light_accent': '#ff00f8', 
                'dark_accent': '#000000', 
                'grey': '#f2f2f2', 
                'postive': '#12d93d', 
                'null': '#ffa800', 
                'negative': '#bf211f'
            }
        )

        body_alt_text = str(job_report_data)

        notifier = EmailHandler(environment = 'notifications')
        notifier.send_email(
            to = recipients,
            subject = subject,
            body_html = body_html,
            body_alt_text = body_alt_text,
            attachments = log_file
        )
        return

def validate_email(original_email: str, domain_check: bool = False) -> tuple[str, None|str]:
    """
    ### validate_email

    Version: 1.1
    Authors: JRA
    Date: 2024-02-12

    #### Explanation:
    Validates an email.

    #### Requirements:
    - email_validator.validate_email

    #### Parameters:
    - original_email (str): Email to be validated.
    - domain_check (bool): If true, the domain of the email is verified. Defaults to false.

    #### Returns:
    - (str): If valid, the normalised email is returned, else the input email is returned.
    - (None|str): Any errors in the email are presented here, None if the email is valid.

    #### Usage:
    >>> validate_email(
            original_email = "JRA.Euler.notifications@gmail.com",
            domain_check = True
        )
    ("JRA.Euler.notifications@gmail.com", None)

    #### History:
    - 1.1 JRA (2024-02-12): Generic exceptions are no longer ignored.
    - 1.0 JRA (2024-01-30): Initial version.
    """
    import email_validator
    try:
        email = email_validator.validate_email(original_email, check_deliverability = domain_check)
    except (email_validator.exceptions_types.EmailNotValidError, email_validator.exceptions_types.EmailSyntaxError, email_validator.exceptions_types.EmailUndeliverableError) as e:
        return (original_email, str(e))
    else:
        return (email.normalized, None)
        
def validate_postcode(original_postcode: str, country: str = "GB") -> tuple[str, None|str]:
    """
    ### validate_postcode

    Version: 1.1
    Authors: JRA
    Date: 2024-02-12

    #### Explanation:
    Validates UK postcodes.

    #### Requirements:
    - postcode_validator.uk.uk_postcode_validator.UKPostcode

    #### Parameters: 
    - original_postcode (str): The postcode to be validated.
    - country (str): The country code of the postcode. Defauts to "GB".

    #### Returns:
    - (str): The postcode in standard format if validated, the original if not.
    - (None|str): Details of errors during validation, None if valid.

    #### Usage:
    >>> validate_postcode("wa11sr")
    "WA1 1SR"

    #### Tasklist:
    - Might be worth having a mapping from country names to codes.

    #### History:
    - 1.1 JRA (2024-02-12): Generic exceptions are no longer ignored.
    - 1.0 JRA (2024-01-30): Initial version.
    """
    from postcode_validator.uk.uk_postcode_validator import UKPostcode
    import postcode_validator.Exceptions.exceptions as postcodeexceptions
    if country == "UK":
        country = "GB"
    if country != "GB":
        return (original_postcode, "Cannot validate postcodes outside of GB.")
    try:
        postcode = UKPostcode(original_postcode)
    except postcodeexceptions.ValidationError as e:
        return (original_postcode, str(e))
    else:
        return (postcode.postcode, None)
    
def validate_phone(original_phone: str, country: str = "GB") -> tuple[str, None|str]:
    """
    ### validate_phone

    Version: 1.1
    Authors: JRA
    Date: 2024-02-12

    #### Explanation: 
    Validates phone numbers.

    #### Requirements:
    - phonenumbers

    #### Parameters:
    - original_phone (str): Phone number to validate.
    - country (str): Country code of phone number. Defaults to "GB".

    #### Returns:
    - (str): Appopriately formatted phone number if valid, the original if not.
    - (None|str): Details of errors during validation, None if valid.

    #### Usage:
    >>> validate_phone("01925644800")
    ('+441925644800', None)

    #### Tasklist:
    - Add a parameter to specify output format of phone number.
    - Might be worth having a mapping from country names to codes.

    #### History:
    - 1.1 JRA (2024-02-12): Generic exceptions are no longer ignored.
    - 1.0 JRA (2024-01-30): Initial version.
    """
    import phonenumbers
    try:
        phone = phonenumbers.parse(original_phone, region = country)
    except phonenumbers.phonenumberutil.NumberParseException as error:
        return (original_phone, str(error))
    else:
        return ("+" + str(phone.country_code) + str(phone.national_number), None)
    
def standardiser(df: pd.DataFrame) -> pd.DataFrame:
    """
    ### standardiser

    Version: 1.0
    Authors: JRA
    Date: 2024-01-30

    #### Explanation:
    Applies validate_email, validate_postcode and validate_phone to a given dataframe by looking for columns "phone", "email" or "postcode".

    #### Requirements:
    - eulib.validate_postcode
    - eulib.validate_phone
    - eulib.validate_email

    #### Parameters:
    - df (pandas.DataFrame): The dataframe to standardise.

    #### Returns:
    - (pandas.DataFrame): Standadised dataframe.

    #### Tasklist:
    - Is this really necessary?
    - Are other fields also worth standardising?

    #### History:
    - 1.0 JRA (2024-01-30): Initial version.
    """
    columns = df.columns.to_list()
    if 'phone' in columns:
        LOG.info("Standardising phone numbers...")
        df[['phone_std', 'phone_error']] = df[['phone', 'country']].apply((lambda x: validate_postcode(x['phone'], x['country'])), axis = 1).to_list()
    if 'postcode' in columns:
        LOG.info("Standardising postcodes...")
        df[['postcode_std', 'postcode_error']] = df[['postcode', 'country']].apply((lambda x: validate_postcode(x['postcode'], x['country'])), axis = 1).to_list()
    if 'email' in columns:
        LOG.info("Standardising email addresses...")
        df[['email_std', 'email_error']] = df['email'].apply((lambda x: validate_postcode(x['email'])), axis = 1).to_list()
    LOG.info("Standardising complete.")
    return df

def mdw_basic_query_builder(source: str, links: list[tuple[str]]) -> tuple[str, dict[str, str]]:
    """
    ### mdw_basic_query_builder
    
    Version: 1.0
    Authors: JRA
    Date: 2024-02-01

    #### Explanation:
    Provided with a source and a list of links, a query is written - with a legend to the aliases - to retrieve live data from hubs, satellites and links.

    #### Requirements:
    - string (package)

    #### Parameters:
    - source (str): The satellites to include.
    - links (list[tuple[str]]): A list of duples of linking semantic types.

    #### Returns:
    - sql (str): The built query.
    - legend (dict[str, str]): A guide to aliases in the query. Aliases are the keys, objects are the values.

    #### Usage:
    >>> mdw_basic_query_builder('source', [('contact', 'address')])[0]
    '''
    SELECT (
            SELECT MAX([loaddate])
            FROM (
                VALUES ([a].[loaddate]),
                    ([b].[loaddate]),
                    ([c].[loaddate]),
                    ([d].[loaddate]),
                    ([e].[loaddate])
            ) AS [T]([loaddate])
        ) AS [loaddate],
        *
    FROM [hub].[contact] AS [a]
        INNER JOIN [sat].[legacyflagscontact] AS [b]
            ON [b].[hashkey] = [a].[hashkey]
        INNER JOIN [link].[contactaddress] AS [c]
            ON [c].[contacthashkey] = [a].[hashkey]
        INNER JOIN [hub].[address] AS [d]
            ON [d].[hashkey] = [c].[addresshashkey]
        INNER JOIN [sat].[legacyflagsaddress] AS [e]
            ON [e].[hashkey] = [d].[hashkey]
    WHERE [a].[lastseendate] IS NULL
        AND [b].[loadenddate] IS NULL
        AND [c].[lastseendate] IS NULL
        AND [d].[lastseendate] IS NULL
        AND [e].[loadenddate] IS NULL
    '''
    >>> mdw_basic_query_builder('source', [('contact', 'address')])[1]
    {'a': '[hub].[contact]', 'b': '[sat].[legacyflagscontact]', 'c': '[link].[contactaddress]', 'd': '[hub].[address]', 'e', '[sat].[legacyflagsaddress]'}

    #### History:
    - 1.0 JRA (2024-02-01): Initial version.
    """
    import string

    legend = {}
    legend['a'] = f"[hub].[{links[0][0]}]"
    legend['b'] = f"[sat].[{source}{links[0][0]}]"
    sql = f"FROM [hub].[{links[0][0]}] AS [{string.ascii_lowercase[0]}]\n\tINNER JOIN [sat].[{source}{links[0][0]}] AS [{string.ascii_lowercase[1]}]\n\t\tON [{string.ascii_lowercase[1]}].[hashkey] = [{string.ascii_lowercase[0]}].[hashkey]"

    aliases = {}
    aliases[links[0][0]] = string.ascii_lowercase[0]
    c = 1
    for link in links:
        c += 3
        check = (link[0] in aliases.keys())
        if check == (link[1] in aliases.keys()):
            raise Exception(f'Link {link} already exists or neither hub has been joined yet.')
        
        next_type = link[1] if check else link[0]

        legend[(c//26 + 1)*string.ascii_lowercase[c - 2]] = f"[link].[{link[0]}{link[1]}]"
        sql += f"\n\tINNER JOIN [link].[{link[0]}{link[1]}] AS [{(c//26 + 1)*string.ascii_lowercase[c - 2]}]\n\t\tON [{(c//26 + 1)*string.ascii_lowercase[c - 2]}].[{link[0] if check else link[1]}hashkey] = [{aliases[link[0]] if check else aliases[link[1]]}].[hashkey]"

        legend[(c//26 + 1)*string.ascii_lowercase[c - 1]] = f"[hub].[{next_type}]"
        sql += f"\n\tINNER JOIN [hub].[{next_type}] AS [{(c//26 + 1)*string.ascii_lowercase[c - 1]}]\n\t\tON [{(c//26 + 1)*string.ascii_lowercase[c - 1]}].[hashkey] = [{(c//26 + 1)*string.ascii_lowercase[c - 2]}].[{next_type}hashkey]"

        legend[(c//26 + 1)*string.ascii_lowercase[c]] = f"[sat].[{source}{next_type}]"
        sql += f"\n\tINNER JOIN [sat].[{source}{next_type}] AS [{(c//26 + 1)*string.ascii_lowercase[c]}]\n\t\tON [{(c//26 + 1)*string.ascii_lowercase[c]}].[hashkey] = [{(c//26 + 1)*string.ascii_lowercase[c - 1]}].[hashkey]"

        aliases[next_type] = string.ascii_lowercase[c - 1]

    loaddate = "SELECT (\n\t\tSELECT MAX([loaddate])\n\t\tFROM (\n\t\t\tVALUES ([a].[loaddate])"
    sql += "\nWHERE [a].[lastseendate] IS NULL"
    for i in range(1, c + 1):
        loaddate += f",\n\t\t\t\t([{string.ascii_lowercase[i]}].[loaddate])"
        sql += f"\n\tAND [{string.ascii_lowercase[i]}].[{'loadenddate' if i%3 == 1 else 'lastseendate'}] IS NULL"
    sql = loaddate + "\n\t\t) AS [T]([loaddate])\n\t) AS [loaddate],\n\t*\n" + sql
    return sql, legend

