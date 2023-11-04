import os
import smtplib
import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
import traceback
import logging
import File_Folder_Inventory_Managment
import Config_File_Inventory_Managment
# Define your email configuration
current_date = datetime.date.today()
Log_date=current_date.strftime("%d%B%Y")
log_file_path = fr"{File_Folder_Inventory_Managment.LogFolder}\ProcessLog_{Log_date}.log"
logging.basicConfig(filename=log_file_path, level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
try:
    logging.info("Log In the Email Through Outlook")
    class EmailConfig:
        EmailTo = Config_File_Inventory_Managment.EmailTo
        EmailCC = Config_File_Inventory_Managment.EmailCC
        SMTPServer = 'smtp-mail.outlook.com'
        SMTPPort = 587
        SMTPUsername = Config_File_Inventory_Managment.EmailFrom
        SMTPPassword = Config_File_Inventory_Managment.EmailPassword

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
            logging.info("Email has sent successfully")
            print("Email sent successfully")
        except Exception as e:
            ErrorMessage = traceback.extract_tb(e.__traceback__)
            line_number = ErrorMessage[-1][1]
            logging.warning(f"Error in Sending Mail: {ErrorMessage} in line {line_number}")
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
        logging.info(f"{file_name} execution is successfully completed on {current_time}")

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
        logging.warning(f"The Error in Process {file_name} on {current_time}")

    def start_mail(file_name):
        current_time= datetime.datetime.now().strftime('%d-%m%-Y at %H:%M:%S')
        body = f"Hello Team,\n\n" + \
               f"{file_name} process started execution on {current_time}\n\n" + \
               "Thanks,\n" + \
               "Python Team"

        subject = "Inventory Managements_Task_Start"
        send_email(subject, body)
        logging.info(f"{file_name} process started execution on {current_time}")

    def filenotexist_mail(file_name):
        # current_time= datetime.datetime.now().strftime('%d-%m%-Y at %H:%M:%S')
        body = f"Hello Team,\n\n" + \
               f"{file_name} does not exist in the 'folder'\n\n" + \
               "Thanks,\n" + \
               "Python Team"

        subject = "Inventory Managements_Process file does not exist"
        send_email(subject, body)
        logging.warning(f"{file_name} does not exist in the 'folder'")

except Exception as e:
    ErrorMessage = traceback.extract_tb(e.__traceback__)
    line_number = ErrorMessage[-1][1]
    logging.error(f"Error in Email Automation File: {ErrorMessage} in line {line_number}")
