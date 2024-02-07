"""
# emailer.py

Version: 1.0
Authors: JRA
Date: 2024-02-06

#### Explanation:
Contains the EmailHandler class for sending emails.

#### Requirements:
- pyjap.logger: Handles logging of processes.
- keyring: For storage and retrieval of keys.
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
- 1.0 JRA (2024-02-07): Initial version.
"""
from pyjap.logger import LOG

import keyring as kr
import smtplib
from os.path import basename
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from email.mime.text import MIMEText

class EmailHandler:
    """
    ## EmailHandler

    Version: 1.0
    Authors: JRA
    Date: 2024-02-07

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

        Version: 1.0
        Authors: JRA
        Date: 2024-02-07

        #### Explanation:
        Initialises the handler and collects parameters.

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
        - 1.0 JRA (2024-02-07): Initial version.
        """
        self.params = {
            'email': email,
            'password': password,
            'smtp': smtp,
            'port': port
        }
        if environment is None and None in [email, password, smtp]:
            LOG.warning("An environment or connection parameters must be specified.")
            return
        elif environment is not None:
            for param in self.params.keys():
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
    
    def connect_to_smtp(self):
        """
        ### connect_to_smtp

        Version: 1.0
        Authors: JRA
        Date: 2024-02-07

        #### Explanation:
        Attempts to connect to the SMTP server.

        #### Usage:
        >>> notifier.connect_to_smtp()

        #### History:
        - 1.0 JRA (2024-02-07): Initial version.
        """
        if self.connected:
            LOG.error("Connection already open.")
            return
        LOG.info(f"Attempting to connect to {self}.")
        try:
            self.connection = smtplib.SMTP(self.params['smtp'], self.params['port'])
            self.connection.starttls()
            self.connection.login(user = self.params['email'], password = self.params['password'])
        except Exception as error:
            LOG.error(f"Failed to connect to {self.params['smtp']}. {error}.")
        else:
            self.connected = True
            LOG.info(f"Successfully connected to {self.params['smtp']}.")
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

        Version: 1.0
        Authors: JRA
        Date: 2024-02-07

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
        - 1.0 JRA (2024-02-07): Initial version.
        """
        if type(to) is str:
            to = [to]
        if type(cc) is str:
            cc = [cc]        
        if type(bcc) is str:
            bcc = [bcc]

        if not self.connected:
            try:
                self.connect_to_smtp()
            except:
                LOG.error(f"Could not connect to {self}. {error}.")
                return
            
        LOG.info(f"Attempting to build email from {self.params['email']}...")
        message_alt = MIMEMultipart('alternative')
                
        message_alt.attach(MIMEText(body_alt_text, "plain"))
        if body_html is not None:
            message_alt.attach(MIMEText(body_html, "html"))
        
        if type(attachments) is str:
            attachments = [attachments]
        if len(attachments) > 0:
            message_mix = MIMEMultipart('mixed')
            message_mix.attach(message_alt)
            for file in attachments:
                try:
                    with open(file, 'rb') as file_reader:
                        part = MIMEApplication(file_reader.read(), name = basename(file))
                    part['Content-Disposition'] = f'attachment; filename={basename(file)}'
                    message_mix.attach(part)
                except Exception as error:
                    LOG.error(f'Could not attach file "{file}". {error}.')
                    return
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
        except Exception as error:
            LOG.error(f"Failed to send email. {error}.")
        else:
            LOG.info("Email sent successfully!")
        self.close_connection()
        return
            
    def close_connection(self):
        """
        ### close_connection

        Version: 1.0
        Authors: JRA
        Date: 2024-02-07

        #### Explanation:
        Closes connections to the SMTP server.

        #### Usage:
        >>> notifier.close_connection()

        #### History:
        - 1.0 JRA (2024-02-07): Initial version.
        """
        if not self.connected:
            LOG.error("No open connection.")
            return
        try:
            self.connection.quit()
        except:
            LOG.error("Failed to close connection.")
        else:
            self.connected = False
            LOG.info(f"Closed connection to {self}.")
        return