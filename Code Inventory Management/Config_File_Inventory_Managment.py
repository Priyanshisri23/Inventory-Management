import pandas as pd
import json
import os
import logging
import datetime
import traceback

current_date = datetime.date.today()
Log_date=current_date.strftime("%d%B%Y")
full_path = os.path.realpath(__file__)
directory_path = os.path.dirname(full_path)
log_file_path = fr"{directory_path}\ProcessLog_{Log_date}.log"
logging.basicConfig(filename=log_file_path, level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
try:
    logging.info(f"The Process has been Started...")
    logging.info(f"Searching for Config File on the Path")
    ConfigFilePath=r"C:\Users\deepak.soni\PycharmProjects\pythonProject\FristPro\Inventory Management\Config\ConfigFile_Inventory_Management.csv"
    if not os.path.exists(ConfigFilePath):
        ConfigFilePath=r"D:\All The Task\Python Project\Inventory Management\Template\ConfigFile_Inventory_Management.csv"
    logging.info("Config file exist in the folder")
    
    try:
        logging.info(f"Fetching Details from Config File...")
        df = pd.read_csv(ConfigFilePath, header=0, index_col=0)
        DataIntoDict = {index: value for index, value in df.iloc[:, 0].items()}
        Get_JSON_Data = json.dumps(DataIntoDict)
        Read_JSON_Data = json.loads(Get_JSON_Data)
        MainFolder = Read_JSON_Data['MainFolder']
        ConfigFolder = Read_JSON_Data['ConfigFolder']
        InputFolder = Read_JSON_Data['InputFolder']
        OutputFolder = Read_JSON_Data['OutputFolder']
        LogFolder = Read_JSON_Data['LogFolder']
        TemplateFolder = Read_JSON_Data['TemplateFolder']
        Bhismafile = Read_JSON_Data['Bhismafile']
        LastQuarterInventoryFile = Read_JSON_Data['LastQuarterInventoryFile']
        MCHAFile = Read_JSON_Data['MCHAFile']
        mch1_v2_datefile = Read_JSON_Data['mch1_v2_datefile']
        PriceListfile = Read_JSON_Data['PriceListfile']
        DivisionSummaryFile = Read_JSON_Data['DivisionSummaryFile']
        AgeingMasterFile = Read_JSON_Data['AgeingMasterFile']
        MB52File = Read_JSON_Data['MB52File']
        ZFIvsGLFile=Read_JSON_Data['ZFIvsGLFile']
        ZFI_ClosingStockFile = Read_JSON_Data['ZFI_ClosingStockFile']
        SLOC_nonproductivelocationFile = Read_JSON_Data['SLOC_nonproductivelocationFile']
        InHouseBhismaNeedleSheet = Read_JSON_Data['InHouseBhismaNeedleSheet']
        QNPLBhismaNeedleSheet = Read_JSON_Data['QNPLBhismaNeedleSheet']
        SutureBhismaNeedleSheet = Read_JSON_Data['SutureBhismaNeedleSheet']
        PRCompileFile = Read_JSON_Data['PRCompileFile']
        EmailTo = Read_JSON_Data['EmailTo']
        EmailCC = Read_JSON_Data['EmailCC']
        EmailFrom = Read_JSON_Data['EmailFrom']
        EmailPassword = Read_JSON_Data['EmailPassword']
        Smtpserver=Read_JSON_Data['Smtpserver']
        SmtpPort=Read_JSON_Data['SmtpPort']

        logging.info("Read all details from Config File")
    except Exception as e:
        ErrorMessage = traceback.extract_tb(e.__traceback__)
        line_number = ErrorMessage[-1][1]
        logging.warning(f"Warning in Config File message: {ErrorMessage} in line {line_number}")
except Exception as e:
    ErrorMessage = traceback.extract_tb(e.__traceback__)
    line_number = ErrorMessage[-1][1]
    logging.error(f"Error in Config File message: {ErrorMessage} in line {line_number}")
