import pandas as pd
import datetime
import logging
import InventoryManagementConfig
import FileFolderInventory

print("Start Time",datetime.datetime.now())
current_date = datetime.date.today()
Log_date=current_date.strftime("%d%B%Y")

try:
    log_file_path = fr"{FileFolderInventory.LogFolder}\ProcessLog_{Log_date}.log"
    logging.basicConfig(filename=log_file_path, level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

    MB52Dumpdf=pd.read_excel(FileFolderInventory.MB52FilePath,header=1)
    mch1df=pd.read_excel(FileFolderInventory.mch1_v2_dateFilePath)
    mchadf=pd.read_excel(FileFolderInventory.MCHAFilePath)
    ZFI_ClosingStockdf=pd.read_excel(FileFolderInventory.ZFI_ClosingStockFilePath,header=1)
    PriceListdf=pd.read_excel(FileFolderInventory.PriceListFilePath,header=1)
    InHouseBhismaNeedledf=pd.read_excel(FileFolderInventory.BhismaFilePath,header=1,sheet_name=InventoryManagementConfig.InHouseBhismaNeedleSheet)
    QNPLBhismaNeedledf=pd.read_excel(FileFolderInventory.BhismaFilePath,header=1,sheet_name=InventoryManagementConfig.QNPLBhismaNeedleSheet)
    SutureBhismaNeedledf=pd.read_excel(FileFolderInventory.BhismaFilePath,header=1,sheet_name=InventoryManagementConfig.SutureBhismaNeedleSheet)
    DivisionSummarydf=pd.read_excel(FileFolderInventory.DivisionSummaryFilePath,header=2)
    SLOC_nonproductivelocationdf=pd.read_excel(FileFolderInventory.SLOC_nonproductivelocationFilePath)
    lastQuarterInventorydf=pd.read_excel(FileFolderInventory.LastQuarterInventoryFilePath)
    ageing_masterdf = pd.read_excel(FileFolderInventory.AgeingMasterFilePath,header=1)

    logging.info('Prepared all input files  dataframe')

    # Column V (Total Stock Quantity) = Sum of column J to O 
    MB52Dumpdf['Total Stock Quantity'] = MB52Dumpdf['Unrestricted'] + MB52Dumpdf['Transit and Transfer'] + MB52Dumpdf['Quality Inspection']+ MB52Dumpdf['Restricted-Use Stock']+ MB52Dumpdf['Blocked']+ MB52Dumpdf['Returns']
    # Column W (Total Stock Value) = Sum of column P to U
    MB52Dumpdf['Total Stock Value'] = MB52Dumpdf['Value Unrestricted'] + MB52Dumpdf['Val. in Trans./Tfr'] + MB52Dumpdf['Value in QualInsp.'] + MB52Dumpdf['Value Restricted'] + MB52Dumpdf['Value BlockedStock']+ MB52Dumpdf['Value Rets Blocked']
    # Column X (Rate per Unit) = Column W/V.
    MB52Dumpdf['Rate per Unit']=MB52Dumpdf["Total Stock Value"] / MB52Dumpdf["Total Stock Quantity"]
    
    MB52Dumpdf['Plant & SKU'] = MB52Dumpdf.apply(lambda row: ''.join(str(col) if pd.notnull(col) else '' for col in [row['Plant'], row['Material']]), axis=1)
    MB52Dumpdf['SKU & Batch'] = MB52Dumpdf.apply(lambda row: ''.join(str(col) if pd.notnull(col) else '' for col in [row['Material'], row['Batch']]), axis=1)
    MB52Dumpdf['Plant SKU & Batch'] = MB52Dumpdf.apply(lambda row: ''.join(str(col) if pd.notnull(col) else '' for col in [row['Plant'], row['Material'], row['Batch']]), axis=1)
    MB52Dumpdf['Plant SKU Batch & SL'] = MB52Dumpdf.apply(lambda row: ''.join(str(col) if pd.notnull(col) else '' for col in [row['Plant'], row['Material'], row['Batch'],row['Storage location']]), axis=1)
    MB52Dumpdf['Mat Desc Batch'] = MB52Dumpdf.apply(lambda row: ''.join(str(col) if pd.notnull(col) else '' for col in [row['Material description'], row['Batch']]), axis=1)

    # mcha1 file concatenation
    mchadf['SKU & Batch'] = mchadf.apply(lambda row: ''.join(str(col) if pd.notnull(col) else '' for col in [row['Material'], row['Batch']]), axis=1)
    
    MB52Dumpdf.insert(29, 'Plant Sku & Val Type','')
    MB52Dumpdf.insert(30, 'Mat dec & valn type','')
    MB52Dumpdf.insert(31, 'SKU & Valn Type','')

    # ==================================Valuation Type=======================================
    # mcha  file   to get the “Valuation Type” column from MCHA file 
    merged_data = MB52Dumpdf.merge(mchadf.drop_duplicates(subset='SKU & Batch'), on='SKU & Batch', how='left')
    MB52Dumpdf['Valan type from MCHA(SKU+Batch)'] = merged_data['Valuation Type']

    # Concatenate
    MB52Dumpdf['Plant Sku & Val Type'] = MB52Dumpdf.apply(lambda row: ''.join(str(col) if pd.notnull(col) else '' for col in [row['Plant'],row['Material'], row['Valan type from MCHA(SKU+Batch)']]), axis=1)
    MB52Dumpdf['Mat dec & valn type'] = MB52Dumpdf.apply(lambda row: ''.join(str(col) if pd.notnull(col) else '' for col in [row['Material description'], row['Valan type from MCHA(SKU+Batch)']]), axis=1)
    MB52Dumpdf['SKU & Valn Type'] = MB52Dumpdf.apply(lambda row: ''.join(str(col) if pd.notnull(col) else '' for col in [row['Material'], row['Valan type from MCHA(SKU+Batch)']]), axis=1)
    
    
    
    # =====================ZFI Closing Stock================
    ZFI_ClosingStockdf['Concatenate(Plant+SKU+Batch)'] = ZFI_ClosingStockdf.apply(lambda row: ''.join(str(col) if pd.notnull(col) else '' for col in [row['Plant'], row['Material'], row['Batch']]), axis=1)
    ZFI_ClosingStockdf['Concatenate(SKU+Batch)'] = ZFI_ClosingStockdf.apply(lambda row: ''.join(str(col) if pd.notnull(col) else '' for col in [row['Material'], row['Batch']]), axis=1)
    ZFI_ClosingStockdf['Concatenate(SKU+Valuation type)'] = ZFI_ClosingStockdf.apply(lambda row: ''.join(str(col) if pd.notnull(col) else '' for col in [row['Material'], row['Valuation Type']]), axis=1)
    ZFI_ClosingStockdf['Concatenate(Mat desc+Valuation type)'] = ZFI_ClosingStockdf.apply(lambda row: ''.join(str(col) if pd.notnull(col) else '' for col in [row['Material description'], row['Valuation Type']]), axis=1)

    output_file_path = rf"{InventoryManagementConfig.OutputFolder}\MB52OutputInventorymanagement.xlsx"
    with pd.ExcelWriter(output_file_path, engine='openpyxl', mode='w') as writer:
        MB52Dumpdf.to_excel(writer,sheet_name='MB52', index=False)

    print("End Time",datetime.datetime.now())
    
except Exception as e:
   logging.info(e)