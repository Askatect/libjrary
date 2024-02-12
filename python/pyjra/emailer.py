"""
# emailer.py

Version: 1.1
Authors: JRA
Date: 2024-02-12

#### Explanation:
Contains the EmailHandler class for sending emails.

#### Requirements:
- pyjap.logger: Handles logging of processes.
- smtplib: To connect to SMTP.
- os.path.basename: Retrieve basenames of any files to attach to emails.
- email.mime.multipart.MIMEMultipart: For building emails.
- email.mime.application.MIMEApplication: For building emails.
- email.mime.test.MIMEText: For building emails.

#### Artefacts:
- EmailHandler (class): Handles emails.

#### Usage:
>>> from pyjap.emailer import EmailHandler

#### History:
- 1.1 JRA (2024-02-12): Revamped error handling and added __del__ to EmailHandler.
- 1.0 JRA (2024-02-07): Initial version.
"""
from pyjap.logger import LOG

import smtplib
from os.path import basename
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from email.mime.text import MIMEText
from socket import gaierror

class EmailHandler:
    """
    ## EmailHandler

    Version: 1.1
    Authors: JRA
    Date: 2024-02-12

    #### Explanation: 
    Handles emails.

    #### Artefacts:
    - params (dict[str, str|int]): Contains SMTP connection parameters.
    - connected (bool): True if connected to SMTP server.
    - connection (smtplib.SMTP): The connection object.
    - __init__ (func): Initialises the handler and collects parameters.
    - __str__ (func): Returns the sending email address.
    - connect_to_smtp (func): Attempts to connect to the SMTP server.
    - send_email (func): Sends an email.
    - close_connection (func): Closes connections to the SMTP server.

    #### Returns:
    - emailer.EmailHandler

    #### Usage:
    >>> notifier = EmailHandler(
            email = 'notifications@domain.ext',
            password = 'password',
            smtp = 'smtp.domain.ext'
            port = 25
        )
    >>> notifier.connect_to_smtp()
    >>> notifier.send_email(
            to = 'recipient@example.com', 
            subject = 'Notification', 
            body_alt_text = 'Get notified.'
        )
    >>> notifier.close_connection()

    #### History:
    - 1.1 JRA (2024-02-12): Revamped error handling and added __del__.
    - 1.0 JRA (2024-02-07): Initial version.
    """
    def __init__(
            self,
            environment: str = None,
            email: str = None,
            password: str = None,
            smtp: str = None,
            port: int = None
    ):
        """
        ### __init__

        Version: 1.1
        Authors: JRA
        Date: 2024-02-12

        #### Explanation:
        Initialises the handler and collects parameters.

        #### Requirements:
        - keyring: For storage and retrieval of keys.

        #### Parameters:
        - environment (str): The environment to collect keyring keys from. Defaults to None.
        - email (str): The sending email address (supercedes environment value). Defaults to None.
        - password (str): The password to use the email address (supercedes environment value). Defaults to None.
        - smtp (str): The name of the SMPT to use (supercedes environment value). Defaults to None.
        - port (int): The port to use (supercedes environment value): Defaults to None.

        #### Usage:
        >>> notifier = EmailHandler(
                email = 'notifications@domain.ext',
                password = 'password',
                smtp = 'smtp.domain.ext'
                port = 25
            )

        #### History:
        - 1.1 JRA (2024-02-12): Exception is raised for insufficient input.
        - 1.0 JRA (2024-02-07): Initial version.
        """
        self.params = {
            'email': email,
            'password': password,
            'smtp': smtp,
            'port': port
        }
        if environment is None and None in [email, password, smtp]:
            error = "Sufficient connection parameters or an environment must be supplied."
            LOG.warning(error)
            raise ValueError(error)
        elif environment is not None:
            for param in self.params.keys():
                import keyring as kr
                value = kr.get_password(environment, param)
                if value is not None:
                    self.params[param] = value
        self.connected = False
        return
    
    def __str__(self) -> str:
        """
        ### __str__

        Version: 1.0
        Authors: JRA
        Date: 2024-02-07

        #### Explanation:
        Returns the sending email address.

        #### Returns:
        - (str)

        #### Usage:
        >>> print(notifier)
        'notifications@domain.ext'

        #### History:
        - 1.0 JRA (2024-02-07): Initial version.
        """
        return self.params['email']
    
    def __del__(self):
        """
        ### __del__

        Version: 1.0
        Authors: JRA
        Date: 2024-02-12

        #### Explanation:
        Closes the SMTP connection before deleting the class instance.

        #### Usage:
        >>> del notifier

        #### History:
        - 1.0 JRA (2024-02-12): Initial version.
        """
        self.close_connection()
        return
    
    def connect_to_smtp(self):
        """
        ### connect_to_smtp

        Version: 2.0
        Authors: JRA
        Date: 2024-02-12

        #### Explanation:
        Attempts to connect to the SMTP server.

        #### Usage:
        >>> notifier.connect_to_smtp()

        #### History:
        - 2.0 JRA (2024-02-12): Revamped error handling.
        - 1.0 JRA (2024-02-07): Initial version.
        """
        if self.connected:
            LOG.error("Connection already open.")
            return
        
        LOG.info(f"Attempting to connect to {self.params['email']} on {self.params['smtp']}...")
        try:
            self.connection = smtplib.SMTP(self.params['smtp'], self.params['port'])
        except gaierror as e:
            LOG.error(f"Could not connect to {self.params['smtp']}{' port' + str(self.params['port']) if self.params['port'] is not None else ''}. {e}")
            raise
        except Exception as e:
            LOG.critical(f"Unexpected {type(e)} error occurred whilst connecting to {self.params['smtp']} port {self.params['port']}. {e}")
            raise
        self.connection.starttls()

        try:
            self.connection.login(user = self.params['email'], password = self.params['password'])
        except smtplib.SMTPAuthenticationError as e:
            LOG.error(f"Failed to login to {self.params['email']}. {e}.")
            raise
        except Exception as e:
            LOG.critical(f"Unexpected {type(e)} error occurred whilst logging in to {self.params['email']}. {e}")
            raise
        else:
            self.connected = True
            LOG.info(f"Successfully connected to {self.params['email']} on {self.params['smtp']}.")
            return
    
    def send_email(
        self,
        subject: str,
        to: str|list[str], 
        cc: str|list[str] = [], 
        bcc: str|list[str] = [], 
        body_html: str = None,
        body_alt_text: str = None,
        attachments: str|list[str] = []
    ):
        """
        ### send_email

        Version: 2.0
        Authors: JRA
        Date: 2024-02-12

        #### Explanation:
        Sends an email.

        #### Parameters:
        - subject (str): The subject of the email.
        - to (str|list[str]): The recipient(s) of the email.
        - cc (str|list[str]): The carbon copy recipient(s) of the email. Defaults to none.
        - bcc (str|list[str]): The blind carbon copy recipient(s) of the email. Defaults to none.
        - body_html (str): The HTML that forms the body of the email.
        - body_alt_text (str): The text that forms the body of the email if the HTML fails or is missing.
        - attachments (str|list[str]): The filepath(s) of attachments.

        #### Usage:
        >>> notifier.send_email(
                to = 'recipient@example.com', 
                subject = 'Notification', 
                body_alt_text = 'Get notified.'
            )

        #### History:
        - 2.0 JRA (2024-02-12): Revamped error handling.
        - 1.0 JRA (2024-02-07): Initial version.
        """
        if type(to) is str:
            to = [to]
        if type(cc) is str:
            cc = [cc]        
        if type(bcc) is str:
            bcc = [bcc]
        if type(attachments) is str:
            attachments = [attachments]

        if not self.connected:
            self.connect_to_smtp()
            
        LOG.info(f"Attempting to build email from {self.params['email']}...")
        message_alt = MIMEMultipart('alternative')                
        message_alt.attach(MIMEText(body_alt_text, "plain"))
        if body_html is not None:
            message_alt.attach(MIMEText(body_html, "html"))
        
        if len(attachments) > 0:
            message_mix = MIMEMultipart('mixed')
            message_mix.attach(message_alt)
            for file in attachments:
                try:
                    file_basename = basename(file)
                    with open(file, 'rb') as file_reader:
                        part = MIMEApplication(file_reader.read(), name = file_basename)
                    part['Content-Disposition'] = f'attachment; filename={file_basename}'
                    message_mix.attach(part)
                except FileNotFoundError as e:
                    LOG.error(f'Could not find "{file_basename}". {e}')
                    raise
                except Exception as error:
                    LOG.critical(f'Unexpected {type(e)} error occurred whilst attaching "{file_basename}". {e}')
                    raise
                else:
                    LOG.info(f'Attached file {file_basename} successfully.')
        else:
            message_mix = message_alt
        
        message_mix["Subject"] = subject
        message_mix["From"] = self.params["email"]
        message_mix["To"] = ",".join(to)
        message_mix["Cc"] = ",".join(cc)
        LOG.info("Email built!")

        try:
            LOG.info(f"Sending email from {self}...")
            self.connection.sendmail(message_mix["From"], to + cc + bcc, message_mix.as_string())
        except Exception as e:
            LOG.error(f"Unexpected {type(e)} error occurred when attempting to send email. {error}.")
            raise
        else:
            LOG.info("Email sent successfully!")
            return
            
    def close_connection(self):
        """
        ### close_connection

        Version: 1.1
        Authors: JRA
        Date: 2024-02-12

        #### Explanation:
        Closes connections to the SMTP server.

        #### Usage:
        >>> notifier.close_connection()

        #### History:
        - 1.1 JRA (2024-02-12): Generic exceptions are now reraised.
        - 1.0 JRA (2024-02-07): Initial version.
        """
        if not self.connected:
            LOG.warning("No open connection to be closed.")
            return
        try:
            self.connection.quit()
        except Exception as e:
            LOG.error(f"Unexpected {type(e)} error occurred when closing connection to {self}. {e}")
            raise
        else:
            self.connected = False
            LOG.info(f"Closed connection to {self}.")
            return