#!/home/alex/.local/lib/python3.6/ # specify python installation on server
# -*- coding: utf-8 -*-
# version 1.0.0

# This code defines the function 'send_email' which is used to send an email 
# from viuhydromet@gmail.com address to a recipient with a CSV file attached
# that contains checks on the Wx station SQL data for the last 24 hours.
# Written by J. Bodart

from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import smtplib

# Email log-in details stored in config_emails file
import config_emails
email_details = config_emails.email_login()

# function that when called in main code enables sending an email with report of
# last 24 hours of data as a CSV attachment
def send_email(csv_filename, df):
    send_from = email_details[0]
    password = email_details[1]
    send_to = 'julien.bodart@viu.ca'
    subject = 'VIU Hydromet daily report'
    message = """\
    <p><body>This is a daily automated message about the 
    current state of the VIU-hydromet data for the last 24 hours.</body></p>
    <p><br/></p>
    """
    multipart = MIMEMultipart()
    multipart["From"] = send_from
    multipart["To"] = send_to
    multipart["Subject"] = subject  
    attachment = MIMEApplication(df.to_csv(sep =',', index=False, index_label=None))
    attachment["Content-Disposition"] = "attachment; filename={}".format(csv_filename)
    multipart.attach(attachment)
    multipart.attach(MIMEText(message, "html"))
    server = smtplib.SMTP("smtp-mail.outlook.com", 587)
    server.starttls()
    server.login(multipart["From"], password)
    server.sendmail(multipart["From"], multipart["To"], multipart.as_string())
    server.quit()