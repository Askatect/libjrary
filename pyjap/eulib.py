"""
# eulib.py

Version: 1.1
Authors: JRA
Date: 2024-02-02

#### Explanation:
Library of functions and classes that might be useful for Euler DataOps and Analytics.

#### Requirements:
- pyjap.logger: Handles logging of processes.
- pyjap.formatting.dataframe_to_html: Converts a dataframe to a pretty HTML table.
- pyjap.email: Email handler.
- pyjap.sql: SQL DB handler.
- pyjap.azureblobstore: Azure blob storage handler.
- pandas: For dataframes.
- uuid: For generating unique identifiers.
- re: Regular expressions.
- datetime.datetime
- pyjap.formatting.tabulate: For pretty text tables.

#### Attributes:
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
- Revise all error handling (no more general exceptions, at least not without reraising).
- Is standardiser necessary?

#### History:
- 1.1 JRA (2024-02-02): Replaced tabulate.tabulate with pyjap.formatting.tabulate. Added mdw_basic_query_builder.
- 1.0 JRA (2024-01-30): Initial version.
"""
from pyjap.logger import LOG

from pyjap.formatting import dataframe_to_html
from pyjap.formatting import tabulate
from pyjap.email import EmailHandler
from pyjap.sql import SQLHandler as SQL
from pyjap.azureblobstore import AzureBlobHandler as AzureBlobs

import pandas as pd
import uuid
import re as regex
from datetime import datetime

def validate_string(string: str, target: str) -> tuple[str, None|str]:
    """
    Version: 1.0
    Authors: JRA
    Date: 2024-01-30

    #### Explanation: Validates a given string against a given regular expression, and can also verify a single datetime within the string using named regular expression groups. The name of the groups should be SQL string datetime format.

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
    - 1.0 JRA (2024-01-30)
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
    except ValueError as error:
        return (string, str(error))
    else:
        return (string, None)

def validate_email(original_email: str, domain_check: bool = False) -> tuple[str, None|str]:
    """
    Version: 1.0
    Authors: JRA
    Date: 2024-01-30

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
    - 1.0 JRA (2024-01-30): Initial version.
    """
    from email_validator import validate_email
    try:
        email = validate_email(original_email, check_deliverability = domain_check)
    except Exception as error:
        return (original_email, str(error))
    else:
        return (email.normalized, None)
        
def validate_postcode(original_postcode: str, country: str = "GB") -> tuple[str, None|str]:
    """
    Version: 1.0
    Authors: JRA
    Date: 2024-01-30

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
    - 1.0 JRA (2024-01-30): Initial version.
    """
    from postcode_validator.uk.uk_postcode_validator import UKPostcode
    if country == "UK":
        country = "GB"
    if country != "GB":
        return (original_postcode, "Cannot validate postcodes outside of GB.")
    try:
        postcode = UKPostcode(original_postcode)
    except Exception as error:
        return (original_postcode, str(error))
    else:
        return (postcode.postcode, None)
    
def validate_phone(original_phone: str, country: str = "GB") -> tuple[str, None|str]:
    """
    Version: 1.0
    Authors: JRA
    Date: 2024-01-30

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
    - 1.0 JRA (2024-01-30): Initial version.
    """
    import phonenumbers
    try:
        phone = phonenumbers.parse(original_phone, region = country)
    except Exception as error:
        return (original_phone, str(error))
    else:
        return ("+" + str(phone.country_code) + str(phone.national_number), None)
    
def standardiser(df: pd.DataFrame) -> pd.DataFrame:
    """
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

class MDWJob:
    """
    Version: 1.0
    Authors: JRA
    Date: 2024-01-30

    #### Explanation:
    Handles jobs in an MDW.

    #### Attributes:
    - mdw (pyjap.sql.SQLHandler): Instance of SQL handler class that is where the job will be executed.
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

    #### Tasklist:
    - Could be better to force start a new job when a new job name is given. Then it wouldn't be necessary to check job status at the start of each private method. It could then be possible to call private methods without starting (and without not ending) a job.

    #### History:
    - 1.0 JRA (2024-01-30): "Initial" version (there have been so many developments, but this is when I'm writing the docstrings).
    """
    def __init__(self, supjob_name: str, mdw: SQL):
        """
        Version: 1.0
        Authors: JRA
        Date: 2024-01-30

        #### Explanation:
        Initialises the MDWJob, creates a supjob identifier and logs the start of the job.

        #### Requirements:
        - MDWJob.supjob_start

        #### Parameters:
        - supjob_name (str): The name of the supjob.
        - mdw (pyjap.sql.SQLHandler): The target MDW to execute the supjob.

        #### Usage:
        >>> MDWJob("source_000_aspect", pyjap.sql.SQLHandler(environment = "dev"))

        #### History:
        - 1.0 JRA (2024-01-30): Initial version.
        """
        self.mdw = mdw
        self.supjob_name = supjob_name

        self.supjob_id = str(uuid.uuid4())
        LOG.info(f"Supjob ID: {self.supjob_id}.")

        self.supjob_start()

        self.job_id = None
        self.job_name = None
        return
    
    def __str__(self):
        """
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
        if self.supjob_name is None:
            return
        self.supjob_end()
        return
    
    def __validate_job_name(self, job_name: str) -> tuple[str, None|str]:
        """
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
    
    def supjob_start(self):
        """
        Version: 1.0
        Authors: JRA
        Date: 2024-01-30

        #### Explanation:
        Logs the start of the supjob.

        #### Requirements:
        - [log].[usp_job_insert]

        #### Usage:
        >>> source_control.supjob_start()

        #### Tasklist:
        - Handle the case where a user attempts to start a supjob in the middle of use. Maybe make this a private method that takes supjob_name as a parameter, and only works if self.supjob_name is not already assigned.

        #### History: 
        - 1.0 JRA (2024-01-30): Initial version.
        """
        LOG.info("Logging start of supjob.")
        self.mdw.execute_query("EXECUTE [log].[usp_job_insert] @jobname = ?, @jobid = ?", values = (self.supjob_name, self.supjob_id))
        return

    def supjob_end(self):
        """
        Version: 1.0
        Authors: JRA
        Date: 2024-01-30

        #### Explanation: 
        Logs the end of the supjob.

        #### Requirements:
        - [log].[usp_job_update]

        #### Usage:
        >>> source_control.supjobstart()

        #### History:
        - 1.0 JRA (2024-01-30): Initial version.
        """
        if self.supjob_name is None:
            LOG.warning("This job has already finished.")
            return
        LOG.info("Logging end of supjob.")
        self.mdw.execute_query("EXECUTE [log].[usp_job_update] @jobid = ?, @status = 'Finished'", values = (self.supjob_id))
        self.supjob_name = None
        return

    def job_start(self, job_name: str, filename: str = None) -> bool:
        """
        Version: 1.0
        Authors: JRA
        Date: 2024-01-30

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
        - 1.0 JRA (2024-01-30): Initial version.
        """
        if self.supjob_name is None:
            LOG.warning('Supjob is not active.')
            return False
        elif job_name is None:
            LOG.warning('Job name must be provided.')
            return False
        elif self.job_name == job_name:
            LOG.info(f'Job "{self.job_name}" is currently running.')
            return True
        elif self.job_name is not None:
            LOG.warning(f'Cannot start a new job while "{self.job_name}" is running!')
            return False
        
        self.job_id = str(uuid.uuid4())
        job_name, error = self.__validate_job_name(job_name)
        if error is None:
            self.job_name = job_name
        else:
            LOG.critical(error)
            return False
        
        try:
            self.mdw.execute_query("EXECUTE [log].[usp_jobdetail_insert] @jobname = ?, @jobdetailid = ?, @jobid = ?, @status = 'Processing', @file_id = ?", values = (self.job_name, self.job_id, self.supjob_id, filename))
        except Exception as error:
            LOG.critical(f'Subjob "{self.job_name}" could not be started. {error}')
            return False
        else:
            LOG.info(f'Subjob "{self.job_name}" started.')
            return True
        
    def job_end(self, status: str, error_message: str = None):
        """
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
        - 1.0 JRA (2024-01-30): Initial version.
        """
        if error_message is not None:
            LOG.warning(error_message)
            self.mdw.execute_query("EXECUTE [log].[usp_log_error] @job_id = ?, @id = ?, @message = ?", values = (self.job_id, self.supjob_id, error_message))
        else:
            LOG.info(f'Finished job "{self.job_name}".')
        
        self.mdw.execute_query("EXECUTE [log].[usp_jobdetail_update] @jobdetailid = ?, @status = ?", values = (self.job_id, status))
        self.job_id = None
        self.job_name = None
        return

    def ingest_csv_blobs_to_mdw(
        self,
        job_name: str, 
        azure_storage: AzureBlobs, 
        container: str, 
        filename_formats: str|list[str],
        reject_bad_filename: bool = True
    ):
        """
        Version: 1.2
        Authors: JRA
        Date: 2024-02-01

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
                azure_storage = pyjap.azureblobstore.AzureBlobHandler(environment = "dev"),
                container = "storage",
                filename_format = r"(?P<yyyy>\d{4})(?P<MM>\d{2})(?P<dd>\d{2})-(\d{7})(\.)Csv",
                reject_bad_filename = False
            )

        #### History:
        - 1.2 JRA (2023-01-02): Fixed an issue where the arguments for __add_artificial_key_file_id were misaligned.
        - 1.1 JRA (2024-01-31): Added functionality for multiple acceptable filename formats.
        - 1.0 JRA (2024-01-30): Initial version.
        """
        directory = self.supjob_name[:(self.supjob_name.find("_"))] + '/'
        filename_formats = [filename_formats] if isinstance(filename_formats, str) else filename_formats

        LOG.info(f'Retrieving blob names from "{container}" at {azure_storage}.')
        filelist = azure_storage.get_blob_names(container)
        filelist = [blob for blob in filelist if blob.startswith(directory) and blob.count('/') == 1]

        for filename in filelist:
            if not self.job_start(job_name, filename):
                continue
        
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
                self.__stage_blob(self.job_name, azure_storage, container, filename)
            except Exception as error:
                self.job_end('Failure', f'Could not stage file "{filename}". {error}')
                continue
            else:
                LOG.info(f'File "{filename}" has been staged in {self.mdw}.')

            LOG.info("Performing structure compliance...")
            try:
                structure_compliance_result = self.__structure_compliance(self.job_name)
            except Exception as error:
                self.job_end('Failure', f'Could not perform structure compliance. {error}')
                continue
            if structure_compliance_result is not None:
                azure_storage.rename_blob(container, filename, filename.replace(directory, directory + 'rejected/'))
                self.job_end('Rejected', f'File did not pass structure compliance. {structure_compliance_result}')
                continue
            else:
                del structure_compliance_result
                LOG.info("No issues found during structure compliance.")

            LOG.info("Adding [FileId], [artificialkey] and [identity] columns to the staged table...")
            try:
                self.__add_artificial_key_file_id(
                    job_name = self.job_name, 
                    job_id = self.job_id,
                    filename = filename
                )
            except Exception as error:
                self.job_end('Failure', f'Could not add key columns to the staged table. {error}')
                continue
            else:
                LOG.info("Key columns added to staged table.")

            LOG.info("Performing staging extraction...")
            try:
                self.__staging_extraction(self.job_name)
            except Exception as error:
                self.job_end('Failure', f'Could not perform staging extraction. {error}')
                continue
            else:
                LOG.info("Staging extraction complete.")

            azure_storage.rename_blob(container, filename, filename.replace(directory, directory + 'archive/'))
            self.job_end('Complete')
        return

    def __stage_blob(
        self, 
        job_name: str, 
        azure_storage: AzureBlobs,
        container: str,
        filename: str
    ):
        """
        Version: 1.0
        Authors: JRA
        Date: 2024-01-30

        #### Explanation:
        Inserts a CSV file into the "stg" schema of the MDW.

        #### Requirements:
        - MDWJob.start_job

        #### Parameters:
        - job_name (str)
        - azure_storage (pyjap.azureblobstore.AzureBlobs)
        - container (str)
        - filename (str)

        #### History: 
        - 1.0 JRA (2024-01-30): Initial version.
        """
        if not self.job_start(job_name):
            raise Exception("Could not start job.")
        
        table = self.job_name + '_' + self.job_id

        LOG.info(f'Retrieving file "{filename}" from "{container}" at {azure_storage}...')
        df = azure_storage.get_blob_csv_as_dataframe(container, filename, encoding = 'latin1', header = None).map(lambda x: None if pd.isnull(x) else str(x))

        df.rename(columns = df.iloc[0], inplace = True)
        df.drop(df.index[0], inplace = True)
        cols = [('\n' + str(1 + i) + ': ' + col) for i, col in enumerate(df.columns)]
        LOG.info(f'Retrieved file "{filename}" with columns: {"".join(cols)}.')

        name_map = {col.lower(): [] for col in df.columns if [col.lower() for col in df.columns].count(col.lower()) > 1}
        if len(name_map) > 0:
            for col in df.columns:
                col_lower = col.lower()
                if col_lower in name_map.keys():
                    name_map[col_lower].append(col + '_' + str(1 + len(name_map[col_lower])))
            cols = [('\n' + col_lower + ' -> ' + str(cols)) for col_lower, cols in name_map.items()]
            LOG.info(f'Renaming duplicate columns: {"".join(cols)}.')
            df = df.rename(columns = (lambda col: name_map[col.lower()].pop(0) if col.lower() in name_map.keys() else col))
            del col_lower
        del name_map
        del cols

        LOG.info(f'Staging file "{filename}" in the MDW.')
        self.mdw.insert('stg', table, df = df, replace_table = True)
        del df
        return
    
    def __structure_compliance(self, job_name: str, job_id: str = None) -> None|str:
        """
        Version: 1.0
        Authors: JRA
        Date: 2024-01-30

        #### Explanation: 
        Executes the structure compliance stored procedure on a staged table.

        #### Requirements:
        - MDWJob.job_start
        - [integration].[usp_structure_compliance]

        #### Parameters:
        - job_name (str)
        - job_id (str)

        #### Returns:
        - (None|str): Details of error are returned, or None if structure is compliant.

        #### History:
        - 1.0 JRA (2024-01-30): Initial version.
        """
        if not self.job_start(job_name):
            raise Exception("Could not start job.")
        return self.mdw.execute_query("EXECUTE [integration].[usp_structure_compliance] @job = ?, @jobid = ?, @schema = ?, @error = '', @print = 0, @display = 1", values = (self.job_name, job_id or self.job_id, 'stg'))[0][0]
    
    def __add_artificial_key_file_id(self, job_name: str, job_id: str = None, filename: str = None):
        """
        Version: 1.0
        Authors: JRA
        Date: 2024-01-30

        #### Explanation: 
        Executes the stored procedure to add [artificialkey], [FileId] and [id] columns to a staged table.
        
        #### Requirements:
        - MDWJob.job_start
        - [integration].[usp_add_artificial_key_file_id]

        #### Parameters:
        - job_name (str)
        - job_id (str)
        - filename (str)

        #### History:
        - 1.0 JRA (2024-01-30): Initial version.
        """
        if not self.job_start(job_name):
            raise Exception("Could not start job.")
        self.mdw.execute_query("EXECUTE [integration].[usp_add_artificial_key_file_id] @job_name = ?, @job_id = ?, @filename = ?, @print = 0", values = (self.job_name, job_id or self.job_id, filename or 'NULL'))
        return
    
    def __staging_extraction(self, job_name: str):
        """
        Version: 1.0
        Authors: JRA
        Date: 2024-01-30

        #### Explanation: 
        Calls a stored procedure that extracts data in a staging table into new staging tables based on their semantic types, and then ingests these into the MDW.

        #### Requirements:
        - MDWJob.job_start
        - [integration].[usp_staging_extraction]

        #### Parameters:
        - job_name (str)

        #### History:
        - 1.0 JRA (2024-01-30): Inital version.
        """
        if not self.job_start(job_name):
            raise Exception("Could not start job.")
        self.mdw.execute_query("EXECUTE [integration].[usp_staging_extraction] @job = ?, @jobid = ?, @supjobid = ?, @schema = ?, @print = 0", values = (self.job_name, self.job_id, self.supjob_id, 'stg'))
        return
    
    def cleaning_transforms(self, job_name: str):
        """
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
        - 1.0 JRA (2024-01-30): Initial version.
        """
        if not self.job_start(job_name):
            return
        try:
            LOG.info(f'Running "{self.job_name}" cleaning transformations on {self.mdw}.')
            cleaning_result = self.mdw.execute_query(
                """
                DECLARE @error nvarchar(max)
                EXECUTE [integration].[usp_cleaning] @jobname = ?, @jobid = ?, @error = @error OUTPUT
                SELECT @error AS [error_detail]
                """, 
                values = (self.job_name, self.job_id)
            )[0][0]
        except Exception as error:
            self.job_end('Failure', f'Python error occurred during cleaning transformations. {error}')
        else:
            if cleaning_result != None:
                self.job_end('Failure', f'SQL error occurred during cleaning transformations. {cleaning_result}')
            else:
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
        azure_storage: AzureBlobs, 
        container: str, 
        folder: str,
        files: str|list[str] = None
    ):
        """
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
        - azure_storage (pyjap.email.EmailHandler): The Azure storage client.
        - container (str): The name of the container to work with in Azure storage.
        - folder (str): The name of the folder within the container to work with in Azure storage.
        - files (str|list[str]): The file(s) to be reset from one level below the given folder to that given folder. Defaults to all possible files.

        #### Usage: 
        >>> source_control.reset_azure_container(
                job_name = "source_002_azurereset",
                azure_storage = pyjap.azureblobstore.AzureBlobHandler(environment = "dev"),
                container = "storage",
                folder = "supfolder",
                files = "supfolder/subfolder/sample.txt"
            )

        #### History: 
        - 1.0 JRA (2024-01-30): Initial version.
        """
        filenames = azure_storage.get_blob_names(container)
        filenames = [filename for filename in filenames if filename.startswith(folder) and filename.count("/") > 1]
        if files is not None:
            files = [files] if isinstance(files, str) else files
            filenames = [filename for filename in filenames if filename in files]
        if not self.job_start(job_name, '; '.join(filenames) if len(filenames) > 0 else None):
            return
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
        if not self.job_start(job_name):
            return
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
        Version: 1.1
        Authors: JRA
        Date: 2024-02-01

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

        #### Tasklist:
        - Maybe write a bespoke pretty tabulator?

        #### History:
        - 1.1 JRA (2024-02-01): Fixed a bug where supjob name is nullified before subject header is written.
        - 1.0 JRA (2024-01-30): Initial version.
        """
        LOG.info(f'Ending and retrieving MDW log information for job {self}.')
        subject = f'Job Report for {self.__str__()}'
        self.supjob_end()
        job_log = self.mdw.select_to_dataframe(
            f"""
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
        )

        job_log = job_log.set_index('Name')
        LOG.info("Writing log information to HTML.")
        body_html = dataframe_to_html(
            job_log, 
            gradient_cols = [], 
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

        notifier = EmailHandler(environment = 'notifications')
        notifier.send_email(
            to = recipients,
            subject = subject,
            body_html = body_html,
            body_alt_text = tabulate(job_log, headers = 'keys', tablefmt = 'psql'),
            attachments = log_file
        )
        return

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
