from pyjap.Logger import LOG

import keyring as kr
import smtplib
from os.path import basename
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from email.mime.text import MIMEText

class EmailHandler:
    """
    EmailHandler - A utility class for sending emails.

    Args:
        environment (str, optional): The environment key for retrieving connection details.
        email (str, optional): The email address.
        password (str, optional): The email account password.
        smtp (str, optional): The SMTP server address.
        port (int, optional): The SMTP server port.

    Attributes:
        params (dict): A dictionary containing the email connection parameters.
        connected (bool): Flag indicating if a connection to the SMTP server is currently open.
        connection: The smtplib.SMTP connection object.

    Methods:
        connect_to_smtp: Connects to the SMTP server.
        send_email: Sends an email with optional HTML content.
        close_connection: Closes the SMTP connection.

    Usage:
        handler = EmailHandler(environment='your_environment')
        handler.connect_to_smtp()
        handler.send_email(to='recipient@example.com', subject='Test Email', body_alt_text='This is a test email.')
        handler.close_connection()
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
        Initializes the EmailHandler instance.

        Args:
            environment (str, optional): The environment key for retrieving connection details.
            email (str, optional): The email address.
            password (str, optional): The email account password.
            smtp (str, optional): The SMTP server address.
            port (int, optional): The SMTP server port.
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
    
    def __str__(self):
        return self.params['email']
    
    def connect_to_smtp(self):
        """
        Connects to the SMTP server.
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
    
    def send_email(self, 
                   to: list|str, 
                   cc: list|str = [], 
                   bcc: list|str = [], 
                   subject: str = "",
                   body_html: str = None,
                   body_alt_text: str = None,
                   attachments: list[str]|str = []
    ):
        """
        Sends an email.

        Args:
            to (list|str): The recipient(s) of the email.
            cc (list|str, optional): The carbon copy recipient(s) of the email.
            bcc (list|str, optional): The blind carbon copy recipient(s) of the email.
            subject (str): The subject of the email.
            body_html (str, optional): The HTML body of the email.
            body_alt_text (str, optional): The alternative plain text body of the email.
            attachments: (list[str]|str): File address(es) to add as email attachments.
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
                return None
            
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
        Closes the SMTP connection.
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