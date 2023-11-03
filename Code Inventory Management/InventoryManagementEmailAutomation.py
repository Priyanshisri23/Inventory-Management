import os
import smtplib
import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
import InventoryManagementConfig
# Define your email configuration

class EmailConfig:
    EmailTo = InventoryManagementConfig.EmailTo
    EmailCC = InventoryManagementConfig.EmailCC
    SMTPServer = 'smtp-mail.outlook.com'
    SMTPPort = 587
    SMTPUsername = InventoryManagementConfig.EmailFrom
    SMTPPassword = InventoryManagementConfig.EmailPassword

def send_email(subject, body, attachment_path=None):
    msg = MIMEMultipart()
    msg['From'] = EmailConfig.SMTPUsername
    msg['To'] = EmailConfig.EmailTo
    msg['Cc'] = EmailConfig.EmailCC
    msg['Subject'] = subject

    msg.attach(MIMEText(body, 'plain'))

    if attachment_path:
        with open(attachment_path, 'rb') as attachment:
            part = MIMEApplication(attachment.read())
            part.add_header('Content-Disposition', f'attachment; filename="{os.path.basename(attachment_path)}"')
            msg.attach(part)
    try:
        server = smtplib.SMTP(EmailConfig.SMTPServer, EmailConfig.SMTPPort)
        server.starttls()
        server.login(EmailConfig.SMTPUsername, EmailConfig.SMTPPassword)
        server.sendmail(msg['From'], msg['To'].split(',') + msg['Cc'].split(','), msg.as_string())
        server.quit()
        print("Email sent successfully")
    except Exception as e:
        print("Error sending email:", e)

def success_mail(file_name, path):
    current_time= datetime.datetime.now().strftime('%d-%m-%Y at %H:%M:%S')
    body = f'''Hello Team,

{file_name} execution is successfully completed on {current_time}

Please find the attached file.

Thanks,
Python Team'''

    subject = "Inventory Managements_Task_Completed"
    send_email(subject, body, path)

def error_mail(file_name, line_number, e):
    current_time= datetime.datetime.now().strftime('%d-%m-%Y at %H:%M:%S')
    body = f"Hello Team,\n\n" + \
           f"Please find the Error in Process {file_name} on {current_time}\n\n" + \
           f"Error Message: {e}\n" + \
           f"Error Line Number: {line_number}\n\n" + \
           "Thanks,\n" + \
           "Python Team"

    subject = "Inventory Managements_Task_Error"
    send_email(subject, body)

def start_mail(file_name):
    current_time= datetime.datetime.now().strftime('%d-%m%-Y at %H:%M:%S')
    body = f"Hello Team,\n\n" + \
           f"{file_name} process started execution on {current_time}\n\n" + \
           "Thanks,\n" + \
           "Python Team"

    subject = "Inventory Managements_Task_Start"
    send_email(subject, body)

def filenotexist_mail(file_name):
    # current_time= datetime.datetime.now().strftime('%d-%m%-Y at %H:%M:%S')
    body = f"Hello Team,\n\n" + \
           f"{file_name} does not exist in the 'folder'\n\n" + \
           "Thanks,\n" + \
           "Python Team"

    subject = "Inventory Managements_Process file does not exist"
    send_email(subject, body)

# start_mail("ExampleProcess")
# success_mail("ExampleProcess", r"D:\All The Task\Python Project\Inventory Management\Input\MB52_Dump.xlsx")
# error_mail("ExampleProcess", 42, "An error occurred")
