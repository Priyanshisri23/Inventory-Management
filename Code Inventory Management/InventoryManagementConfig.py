import pandas as pd
import json
import os
import logging
import datetime

current_date = datetime.date.today()
Log_date=current_date.strftime("%d%B%Y")
full_path = os.path.realpath(__file__)
directory_path = os.path.dirname(full_path)
log_file_path = fr"{directory_path}\ProcessLog_{Log_date}.log"
logging.basicConfig(filename=log_file_path, level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
try:
    ConfigFilePath=r"D:\All The Task\Python Project\Inventory Management\Config\ConfigFile_Inventory_Management.csv"
    if not os.path.exists(ConfigFilePath):
        ConfigFilePath=r"D:\All The Task\Python Project\Inventory Management\Template\ConfigFile_Inventory_Management.csv"
    logging.info("Config file exist in the folder")
except Exception as e:
    logging.error(e)
try:
    df = pd.read_csv(ConfigFilePath, header=0, index_col=0)
    c = {index: value for index, value in df.iloc[:, 0].items()}
    json_c = json.dumps(c)
    data = json.loads(json_c)
    MainFolder = data['MainFolder']
    ConfigFolder = data['ConfigFolder']
    InputFolder= data['InputFolder']
    OutputFolder= data['OutputFolder']
    LogFolder= data['LogFolder']
    TemplateFolder=data['TemplateFolder']
    Bhismafile= data['Bhismafile']
    LastQuarterInventoryFile= data['LastQuarterInventoryFile']
    MCHAFile= data['MCHAFile']
    mch1_v2_datefile= data['mch1_v2_datefile']
    InputFolder= data['InputFolder']
    PriceListfile=data['PriceListfile']
    DivisionSummaryFile=data['DivisionSummaryFile']
    AgeingMasterFile=data['AgeingMasterFile']
    MB52File=data['MB52File']
    ZFI_ClosingStockFile=data['ZFI_ClosingStockFile']
    SLOC_nonproductivelocationFile=data['SLOC_nonproductivelocationFile']
    InHouseBhismaNeedleSheet=data['InHouseBhismaNeedleSheet']
    QNPLBhismaNeedleSheet= data['QNPLBhismaNeedleSheet']
    SutureBhismaNeedleSheet = data['SutureBhismaNeedleSheet']
    EmailTo= data['EmailTo']
    EmailCC= data['EmailCC']
    EmailFrom= data['EmailFrom']
    EmailPassword = data['EmailPassword']
    logging.info("Read all details from Config File")
except Exception as e:
    logging.error(e)

    