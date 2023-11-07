#!/home/alex/.local/lib/python3.6/ # specify python installation on server
# -*- coding: utf-8 -*-
# version 1.0.0

# This code defines the function 'send_email' which is used to send an email 
# from viuhydromet@outlook.com address to a recipient to alert that data
# is not transmitting (either satellite transmission issue or issue with codes 
# that plot the data onto the VIU hydromet website) for at least the last 6 hours
# Written by J. Bodart

from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import smtplib

# Email log-in details stored in config_emails file
import config_emails
email_details = config_emails.email_login()

# function that when called in main code enables sending an email with alert
def send_email(df):
    send_from = email_details[0]
    password = email_details[1]
    subject = 'Alert transmission issue VIU hydromet - last 6 hours'
    message = """\
    <html>
      <head></head>
      <body>
        {0}
      </body>
    </html>
    """.format(df.to_html(index=False, header=False))
    multipart = MIMEMultipart()
    multipart["From"] = send_from
    multipart["To"] = "julien.bodart@viu.ca,anna.kaveney@viu.ca"
    multipart["Subject"] = subject  
    multipart.attach(MIMEText(message, "html"))
    server = smtplib.SMTP("smtp-mail.outlook.com", 587)
    server.starttls()
    server.login(multipart["From"], password)
    server.sendmail(multipart["From"], multipart["To"].split(","), multipart.as_string())
    server.quit()