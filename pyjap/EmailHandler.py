import logging
import logger

import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import keyring as kr

class EmailHandler:
    def __init__(
            self,
            environment: str = None,
            email: str = None,
            password: str = None,
            smtp: str = None,
            port: int = None
    ):
        self.params = locals()
        self.params.pop('self')
        self.params.pop('environment')
        if environment is not None:
            for param in self.params.keys():
                value = kr.get_password(environment, param)
                if value is not None:
                    self.params[param] = value
        self.connected = False
        self.connect_to_smtp()
        return
    
    def connect_to_smtp(self):
        if self.connected:
            logging.warning("Connection already open.")
            return
        logging.info(f"Attempting to connect to {self.params['email']} on {self.params['smtp']}.")
        try:
            self.connection = smtplib.SMTP(self.params['smtp'], self.params['port'])
            self.connection.starttls()
            self.connection.login(user = self.params['email'], password = self.params['password'])
        except:
            logging.error(f"Failed to connect to {self.params['smtp']}.")
        else:
            self.connected = True
            logging.info(f"Successfully connected to {self.params['smtp']}.")
        return
    
    def send_email(self, 
                   to: list|str, 
                   cc: list|str = [], 
                   bcc: list|str = [], 
                   subject: str = None,
                   body_html: str = None,
                   body_alt_text: str = None
    ):
        if not self.connected:
            logging.warning("No open connection.")
            return
        logging.info(f"Attempting to send email from {self.params['email']}...")
        if type(to) is str:
            to = [to]
        if type(cc) is str:
            cc = [cc]
        if type(bcc) is str:
            bcc = [bcc]
        message = MIMEMultipart("alternative")
        message["From"] = self.params['email']
        message["To"] = ",".join(to)
        message["Cc"] = ",".join(cc)
        message["Subject"] = subject
        message.attach(MIMEText(body_alt_text, "plain"))
        message.attach(MIMEText(body_html, "html"))
        try:
            self.connection.sendmail(self.params['email'], to + cc + bcc, message.as_string())
        except:
            logging.error("Failed to send email.")
        else:
            logging.info("Email sent successfully.")
        return
            
    def close_connection(self):
        if not self.connected:
            logging.warning("No open connection.")
            return
        try:
            self.connection.quit()
        except:
            logging.error("Failed to close connection.")
        finally:
            self.connected = False
            logging.info(f"Closed connection to {self.params['email']} on {self.params['email']}.")
        return
        
test = EmailHandler(None, 'joshua.r.appleton@outlook.com', 'Org@n!53', 'smtp-mail.outlook.com', 587)
test.send_email(['joshua.rueben@hotmail.co.uk'], 
                [],
                'osujah@gmail.com',
                'Test Email',
                'Just <strong>checking</strong> some HTML.',
                "<strong>Alternative</strong> text.")
test.close_connection()