import os
import datetime
import shutil
import logging
import traceback
import Config_File_Inventory_Managment
import Email_Automation_Inventory_Management

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

    try:
        logging.info(f"Start Creating Files and Folders")
        # Creating The Inventory ManagementConfig folder if it does not exist
        InventoryManagementFolder = Config_File_Inventory_Managment.MainFolder
        if not os.path.exists(InventoryManagementFolder):
            os.mkdir(InventoryManagementFolder)

        # Create Config Folder if it does not exist  and the config file if it does not  exist
        ConfigFolder = Config_File_Inventory_Managment.ConfigFolder
        if not os.path.exists(ConfigFolder):
            os.mkdir(ConfigFolder)
        ConfigFile = ConfigFolder + '\ConfigFile_Inventory_Management.csv'
        ConfigFileTemplate = Config_File_Inventory_Managment.TemplateFolder + '\ConfigFile_Inventory_Management.csv'
        if not os.path.exists(ConfigFile):
            shutil.copy(ConfigFileTemplate, ConfigFile)
        logging.info(f"Inventroy Managment Config File and Folder Created")

        # Create Input Folder if it does not exist
        InputFolder = Config_File_Inventory_Managment.InputFolder
        if not os.path.exists(InputFolder):
            Email_Automation_Inventory_Management.filenotexist_mail(InputFolder)
            exit()
        logging.info("Input Folder Created")

        # Create Output Folder if it does not exist
        OutputFolder = Config_File_Inventory_Managment.OutputFolder
        if not os.path.exists(OutputFolder):
            os.mkdir(OutputFolder)
        logging.info("Output Folder Created")

        # Create log Folder if it does not exist
        LogFolder = Config_File_Inventory_Managment.LogFolder
        if not os.path.exists(LogFolder):
            os.mkdir(LogFolder)
        logging.info("Log Folder Created")

        def construct_file_path(base_path):
            placeholders = ["mmmmyyyy", "mmmyy"]
            replacements = [Monthyear_mmmm_yyyy, Monthyear_mmm_yy]
            file_path = os.path.join(InputFolder, base_path)
            for placeholder, replacement in zip(placeholders, replacements):
                file_path = file_path.replace(placeholder, replacement)
            return file_path

        MB52FilePath = construct_file_path(Config_File_Inventory_Managment.MB52File)
        LastQuarterInventoryFilePath = construct_file_path(Config_File_Inventory_Managment.LastQuarterInventoryFile)
        MCHAFilePath = construct_file_path(Config_File_Inventory_Managment.MCHAFile)
        mch1_v2_dateFilePath = construct_file_path(Config_File_Inventory_Managment.mch1_v2_datefile)
        PriceListFilePath = construct_file_path(Config_File_Inventory_Managment.PriceListfile)
        DivisionSummaryFilePath = construct_file_path(Config_File_Inventory_Managment.DivisionSummaryFile)
        AgeingMasterFilePath = construct_file_path(Config_File_Inventory_Managment.AgeingMasterFile)
        ZFI_ClosingStockFilePath = construct_file_path(Config_File_Inventory_Managment.ZFI_ClosingStockFile)
        SLOC_nonproductivelocationFilePath = construct_file_path(
            Config_File_Inventory_Managment.SLOC_nonproductivelocationFile)
        BhismaFilePath = construct_file_path(Config_File_Inventory_Managment.Bhismafile)
        logging.info('Renaming the Files Name')
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
                Email_Automation_Inventory_Management.filenotexist_mail(description)
                exit()
        logging.info("Checked, All Input File exist in Input folder")
    except Exception as e:
        ErrorMessage = traceback.extract_tb(e.__traceback__)
        line_number = ErrorMessage[-1][1]
        logging.warning(f"Warning in File & Folder message: {ErrorMessage} in line {line_number}")
except Exception as e:
    ErrorMessage = traceback.extract_tb(e.__traceback__)
    line_number = ErrorMessage[-1][1]
    logging.error(f"Error in File & Folder message: {ErrorMessage} in line {line_number}")
