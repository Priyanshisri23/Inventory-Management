import os
import datetime
import shutil
import logging
import InventoryManagementConfig
import InventoryManagementEmailAutomation

current_date = datetime.date.today()
Log_date = current_date.strftime("%d%B%Y")
Month = current_date.strftime("%B")
Monthyear_mmm_yy = current_date.strftime("%b%y")
Monthyear_mmmm_yyyy = current_date.strftime("%B%Y")

try:
    full_path = os.path.realpath(__file__)
    directory_path = os.path.dirname(full_path)
    log_file_path = os.path.join(directory_path, f"ProcessLog_{Log_date}.log")
    logging.basicConfig(filename=log_file_path, level=logging.DEBUG,format='%(asctime)s - %(levelname)s - %(message)s')
except Exception as e:
    logging.error(e)

try: 
    #Creating The Inventory ManagementConfig folder if it does not exist
    InventoryManagementFolder = InventoryManagementConfig.MainFolder
    if not os.path.exists(InventoryManagementFolder):
        os.mkdir(InventoryManagementFolder)
    
    # Create Config Folder if it does not exist  and the config file if it does not  exist
    ConfigFolder = InventoryManagementConfig.ConfigFolder
    if not os.path.exists(ConfigFolder):
        os.mkdir(ConfigFolder)
    ConfigFile=ConfigFolder+'\ConfigFile_Inventory_Management.csv'
    ConfigFileTemplate=InventoryManagementConfig.TemplateFolder+'\ConfigFile_Inventory_Management.csv'
    if not os.path.exists(ConfigFile):
        shutil.copy(ConfigFileTemplate, ConfigFile)
    
    # Create Input Folder if it does not exist
    InputFolder = InventoryManagementConfig.InputFolder
    if not os.path.exists(InputFolder):
        logging.info("Input folder does not exist")
        InventoryManagementEmailAutomation.filenotexist_mail(InputFolder)
        exit()
        
    # Create Output Folder if it does not exist
    OutputFolder = InventoryManagementConfig.OutputFolder
    if not os.path.exists(OutputFolder):
        os.mkdir(OutputFolder)       
         
    # Create log Folder if it does not exist
    LogFolder = InventoryManagementConfig.LogFolder
    if not os.path.exists(LogFolder):
        os.mkdir(LogFolder)
        
    def construct_file_path(base_path):
        placeholders = ["mmmmyyyy", "mmmyy"]
        replacements = [Monthyear_mmmm_yyyy, Monthyear_mmm_yy]
        file_path = os.path.join(InputFolder, base_path)
        for placeholder, replacement in zip(placeholders, replacements):
            file_path = file_path.replace(placeholder, replacement)
        return file_path   
    
    MB52FilePath = construct_file_path(InventoryManagementConfig.MB52File)
    LastQuarterInventoryFilePath = construct_file_path(InventoryManagementConfig.LastQuarterInventoryFile)
    MCHAFilePath = construct_file_path(InventoryManagementConfig.MCHAFile)
    mch1_v2_dateFilePath=construct_file_path(InventoryManagementConfig.mch1_v2_datefile)
    PriceListFilePath=construct_file_path(InventoryManagementConfig.PriceListfile)
    DivisionSummaryFilePath=construct_file_path(InventoryManagementConfig.DivisionSummaryFile)
    AgeingMasterFilePath=construct_file_path(InventoryManagementConfig.AgeingMasterFile)
    ZFI_ClosingStockFilePath=construct_file_path(InventoryManagementConfig.ZFI_ClosingStockFile)
    SLOC_nonproductivelocationFilePath=construct_file_path(InventoryManagementConfig.SLOC_nonproductivelocationFile)
    BhismaFilePath=construct_file_path(InventoryManagementConfig.Bhismafile)
    logging.info('Rename input file name')
    file_paths = {
    MB52FilePath: 'MB52 File',
    LastQuarterInventoryFilePath: 'Last Quarter Inventory File',
    MCHAFilePath: 'MCHA File',
    mch1_v2_dateFilePath: 'mch1 file',
    PriceListFilePath: 'Price List File',
    DivisionSummaryFilePath: 'Division Summary File',
    AgeingMasterFilePath: 'Ageing Master File',
    ZFI_ClosingStockFilePath: 'ZFI_Closing Stock File',
    SLOC_nonproductivelocationFilePath: 'SLOC_nonproductivelocation File',
    BhismaFilePath: 'All Plant Needle (Bhisma) File'
}

    for file_path, description in file_paths.items():
        if not os.path.exists(file_path):
            InventoryManagementEmailAutomation.filenotexist_mail(description)
            exit()
    logging.info("Check all Input File exist in Input folder")
except Exception as e:
    logging.error(e)