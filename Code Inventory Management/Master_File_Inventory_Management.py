import pandas as pd
import datetime
import logging
import traceback
import Config_File_Inventory_Managment
import File_Folder_Inventory_Managment

print("Start Time : ",datetime.datetime.now())
current_date = datetime.date.today()
Log_date=current_date.strftime("%d%B%Y")

try:
    Log_file_path = fr"{File_Folder_Inventory_Managment.LogFolder}\ProcessLog_{Log_date}.log"
    logging.basicConfig(filename=Log_file_path, level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
    logging.info("Master File Process has been Started")
    
    MB52Dumpdf = pd.read_excel(File_Folder_Inventory_Managment.MB52FilePath,header=1)
    print('mb52: ',datetime.datetime.now())
    mch1df = pd.read_excel(File_Folder_Inventory_Managment.mch1_v2_dateFilePath)
    print('mch1 : ',datetime.datetime.now()) 
    mchadf = pd.read_excel(File_Folder_Inventory_Managment.MCHAFilePath)
    print('mchadf:',datetime.datetime.now())
    print(File_Folder_Inventory_Managment.ZFI_ClosingStockFilePath)
    ZFI_ClosingStockdf = pd.read_excel(File_Folder_Inventory_Managment.ZFI_ClosingStockFilePath,header=1)
    print('ZFI_ClosingStockdf:',datetime.datetime.now())
    PriceListdf = pd.read_excel(File_Folder_Inventory_Managment.PriceListFilePath,header=1)
    print('PriceListdf:',datetime.datetime.now())
    InHouseBhismaNeedledf = pd.read_excel(File_Folder_Inventory_Managment.BhismaFilePath,header=1,sheet_name=Config_File_Inventory_Managment.InHouseBhismaNeedleSheet)
    QNPLBhismaNeedledf = pd.read_excel(File_Folder_Inventory_Managment.BhismaFilePath,header=1,sheet_name=Config_File_Inventory_Managment.QNPLBhismaNeedleSheet)
    SutureBhismaNeedledf = pd.read_excel(File_Folder_Inventory_Managment.BhismaFilePath,header=1,sheet_name=Config_File_Inventory_Managment.SutureBhismaNeedleSheet)
    print('Bhisma Needle :',datetime.datetime.now())
    DivisionSummarydf = pd.read_excel(File_Folder_Inventory_Managment.DivisionSummaryFilePath,header=2)
    SLOC_nonproductivelocationdf = pd.read_excel(File_Folder_Inventory_Managment.SLOC_nonproductivelocationFilePath)
    print('SLOC_nonproductivelocation :',datetime.datetime.now())
    lastQuarterInventorydf = pd.read_excel(File_Folder_Inventory_Managment.LastQuarterInventoryFilePath,header=1)
    print('Last Quarter Inventory :',datetime.datetime.now())
    ageing_masterdf = pd.read_excel(File_Folder_Inventory_Managment.AgeingMasterFilePath,header=1)
    print('Ageing master: ',datetime.datetime.now())
    ZFIvsGLdf = pd.read_excel(File_Folder_Inventory_Managment.ZFIvsGLFilePath)
    print('ZFIvsGLdf:',datetime.datetime.now())
    print('start :',datetime.datetime.now())
    logging.info('Prepared all input files into dataframe')

    # Column V (Total Stock Quantity) = Sum of column J to O 
    MB52Dumpdf['Total Stock Quantity'] = MB52Dumpdf['Unrestricted'] + MB52Dumpdf['Transit and Transfer'] + MB52Dumpdf['Quality Inspection']+ MB52Dumpdf['Restricted-Use Stock']+ MB52Dumpdf['Blocked']+ MB52Dumpdf['Returns']
    # Column W (Total Stock Value) = Sum of column P to U
    MB52Dumpdf['Total Stock Value'] = MB52Dumpdf['Value Unrestricted'] + MB52Dumpdf['Val. in Trans./Tfr'] + MB52Dumpdf['Value in QualInsp.'] + MB52Dumpdf['Value Restricted'] + MB52Dumpdf['Value BlockedStock']+ MB52Dumpdf['Value Rets Blocked']
    # Column X (Rate per Unit) = Column W/V.
    MB52Dumpdf['Rate P.u.']=MB52Dumpdf["Total Stock Value"] / MB52Dumpdf["Total Stock Quantity"]
    
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
    logging.info("Merged the Columns and Created New Column")

    # ==================================Valuation Type=======================================
    # mcha  file   to get the “Valuation Type” column from MCHA file 
    merged_data = MB52Dumpdf.merge(mchadf.drop_duplicates(subset='SKU & Batch'), on='SKU & Batch', how='left')
    MB52Dumpdf['Valan type from MCHA(SKU+Batch)'] = merged_data['Valuation Type']
    MB52Dumpdf['Valan type from MCHA(SKU+Batch)'] = MB52Dumpdf['Valan type from MCHA(SKU+Batch)'].fillna('NA')

    # Concatenate
    MB52Dumpdf['Plant Sku & Val Type'] = MB52Dumpdf.apply(lambda row: ''.join(str(col) if pd.notnull(col) else '' for col in [row['Plant'],row['Material'], row['Valan type from MCHA(SKU+Batch)']]), axis=1)
    MB52Dumpdf['Mat dec & valn type'] = MB52Dumpdf.apply(lambda row: ''.join(str(col) if pd.notnull(col) else '' for col in [row['Material description'], row['Valan type from MCHA(SKU+Batch)']]), axis=1)
    MB52Dumpdf['SKU & Valn Type'] = MB52Dumpdf.apply(lambda row: ''.join(str(col) if pd.notnull(col) else '' for col in [row['Material'], row['Valan type from MCHA(SKU+Batch)']]), axis=1)
    

    # ===================================ZFI Closing Stock=====================================
    ZFI_ClosingStockdf['Plant SKU & Batch'] = ZFI_ClosingStockdf.apply(lambda row: ''.join(str(col) if pd.notnull(col) else '' for col in [row['Plant'], row['Material'], row['Batch']]), axis=1)
    ZFI_ClosingStockdf['SKU & Batch'] = ZFI_ClosingStockdf.apply(lambda row: ''.join(str(col) if pd.notnull(col) else '' for col in [row['Material'], row['Batch']]), axis=1)
    ZFI_ClosingStockdf['SKU & Valn Type'] = ZFI_ClosingStockdf.apply(lambda row: ''.join(str(col) if pd.notnull(col) else '' for col in [row['Material'], row['Valuation Type']]), axis=1)
    ZFI_ClosingStockdf['Concatenate(Mat desc+Valuation type)'] = ZFI_ClosingStockdf.apply(lambda row: ''.join(str(col) if pd.notnull(col) else '' for col in [row['Material description'], row['Valuation Type']]), axis=1)
    ZFI_ClosingStockdf['Rate per Unit']=ZFI_ClosingStockdf["Total Value"] / ZFI_ClosingStockdf["Total Stock"]
        
        
    merged_data = MB52Dumpdf.merge(ZFI_ClosingStockdf.drop_duplicates(subset='Plant SKU & Batch'), on='Plant SKU & Batch', how='left')
    MB52Dumpdf['Rate used Basis'] = 'Plant SKU & Batch'
    MB52Dumpdf['ZFI Rate'] = merged_data['Rate per Unit']


    merged_data = MB52Dumpdf.merge(ZFI_ClosingStockdf.drop_duplicates(subset='SKU & Batch'), on='SKU & Batch', how='left')
    MB52Dumpdf.loc[MB52Dumpdf['ZFI Rate'].isnull(), 'Rate used Basis'] = 'SKU & Batch'
    MB52Dumpdf.loc[MB52Dumpdf['ZFI Rate'].isnull(), 'ZFI Rate'] = merged_data['Rate per Unit']

    merged_data = MB52Dumpdf.merge(ZFI_ClosingStockdf.drop_duplicates(subset='SKU & Valn Type'), on='SKU & Valn Type', how='left')
    MB52Dumpdf.loc[MB52Dumpdf['ZFI Rate'].isnull(), 'Rate used Basis'] = 'SKU & Valn Type'
    MB52Dumpdf.loc[MB52Dumpdf['ZFI Rate'].isnull(), 'ZFI Rate'] = merged_data['Rate per Unit']

    merged_data = MB52Dumpdf.merge(ZFI_ClosingStockdf.drop_duplicates(subset='Material'), on='Material', how='left')
    MB52Dumpdf.loc[MB52Dumpdf['ZFI Rate'].isnull(), 'Rate used Basis'] = 'SKU'
    MB52Dumpdf.loc[MB52Dumpdf['ZFI Rate'].isnull(), 'ZFI Rate'] = merged_data['Rate per Unit']


    MB52Dumpdf['ZFI Value as of Jun23'] = MB52Dumpdf['ZFI Rate'] * MB52Dumpdf['Total Stock Quantity']
    # Create a newcolumn  Material Group Desc
    merged_dataMGD = MB52Dumpdf.merge(ZFI_ClosingStockdf.drop_duplicates(subset='Material'), on='Material', how='left')
    MB52Dumpdf['Material Group Desc.'] = merged_dataMGD['Material Group Desc.']
    
    # ==========================ZFI vs Gl Sheet working==========================================
    data = {
    'Particulars': ['RM / PM', 'WIP', 'FG'],
    'Amt as per GL': "",  
    'Amt as per ZFI': "", 
    'Amt as per MB52': "",
    'ZFI Vs GL': "", 
    'MB52 Vs GL': "",
}
    
    # Create Pivot table for ZFI_Closing_Stockdf
    pivot_tableZFI = ZFI_ClosingStockdf.pivot_table(values='Total Value', index='Material type', aggfunc='sum')
    pivot_tableZFI.reset_index(inplace=True)

    # Create Pivot table for MB52Dumpdf
    pivot_tableMB52 = MB52Dumpdf.pivot_table(
        values='ZFI Value as of Jun23',
        index='Material type',
        aggfunc='sum'
    )
    
    def categorize_material(material_type):
        if str(material_type) in ['FERT', 'HAWA']:
            return 'FG'
        elif str(material_type) == 'HALB':
            return 'WIP'
        elif str(material_type).startswith('Z'):
            return 'RM / PM'
        else:
            return 'Other'

    pivot_tableZFI['Category'] = pivot_tableZFI['Material type'].map(categorize_material)
    pivot_tableMB52['Category'] = pivot_tableMB52.index.map(categorize_material)
    results = []
    for particular in data['Particulars']:
        filtered_df = ZFIvsGLdf[ZFIvsGLdf['Material Type'] == particular]
        amt_as_per_gl = filtered_df['Amount'].sum() / 10**7
        
        ZFIfiltered_df = pivot_tableZFI[pivot_tableZFI['Category'] == particular]
        amt_as_per_zfi = ZFIfiltered_df['Total Value'].sum() / 10**7
        Mb52filtered_df = pivot_tableMB52[pivot_tableMB52['Category'] == particular]
        amt_as_per_mb52 = Mb52filtered_df['ZFI Value as of Jun23'].sum() / 10**7
        results.append({'Particulars': particular, 'Amt as per GL': amt_as_per_gl,'Amt as per ZFI':amt_as_per_zfi,'Amt as per MB52':amt_as_per_mb52})

    result_df = pd.DataFrame(results)
    result_df['ZFI Vs GL'] = result_df['Amt as per GL'] - result_df['Amt as per ZFI']
    result_df['MB52 Vs GL'] = result_df['Amt as per GL'] - result_df['Amt as per MB52']
    
    # ======================================Under Valuation====================================
    
    MB52Dumpdf['Plant'] = MB52Dumpdf['Plant'].astype(str)
    filtered_df = MB52Dumpdf[MB52Dumpdf['Plant'].isin(['1012','1014'])]
    filteredundervaluation_df = filtered_df.loc[:, :'Material Group Desc.']    
    
    merged_dataa = []
    for i, row in filteredundervaluation_df.iterrows():
        PSBrevisedratematched=lastQuarterInventorydf[lastQuarterInventorydf['Plant SKU & Batch'] == row['Plant SKU & Batch']]
        SBrevisedratematched=lastQuarterInventorydf[lastQuarterInventorydf['SKU & Batch'] == row['SKU & Batch']]
        SVrevisedratematched=lastQuarterInventorydf[lastQuarterInventorydf['SKU & Valn Type'] == row['SKU & Valn Type']]
        Mrevisedratematched=lastQuarterInventorydf[lastQuarterInventorydf['Material'] == row['Material']]
        if not PSBrevisedratematched["Revised Rate"].empty:
            row['Revised Rate'] = PSBrevisedratematched["Revised Rate"].values[0]
        elif not SBrevisedratematched["Revised Rate"].empty:
            row['Revised Rate'] = SBrevisedratematched["Revised Rate"].values[0]
        elif not SVrevisedratematched["Revised Rate"].empty:
            row['Revised Rate'] = SVrevisedratematched["Revised Rate"].values[0]
        elif not Mrevisedratematched["Revised Rate"].empty:
            row['Revised Rate'] = Mrevisedratematched["Revised Rate"].values[0]
        else:
            row['Revised Rate'] = row['ZFI Rate']
        merged_dataa.append(row)
    filteredundervaluation_df=pd.DataFrame(merged_dataa)
    filteredundervaluation_df['Revised Value'] = filteredundervaluation_df['Revised Rate'] * filteredundervaluation_df['Total Stock Quantity']
    filteredundervaluation_df['Under valn'] = filteredundervaluation_df['ZFI Value as of Jun23'] - filteredundervaluation_df['Revised Value']
    
    output_file_path = rf"{Config_File_Inventory_Managment.OutputFolder}\MB52Dump.xlsx"
    with pd.ExcelWriter(output_file_path, engine='openpyxl', mode='w') as writer:
        filteredundervaluation_df.to_excel(writer, sheet_name='Undervaluation', index=False)
        MB52Dumpdf.to_excel(writer,sheet_name='MB52', index=False)
        ZFI_ClosingStockdf.to_excel(writer,sheet_name='ZFI_Closing_Stock', index=False)
        ZFIvsGLdf.to_excel(writer, sheet_name='ZFIvsGL', index=False)
        pivot_tableZFI.to_excel(writer, sheet_name='ZFIvsGL', startrow=len(ZFIvsGLdf) + 2, index=False)
        pivot_tableMB52.to_excel(writer, sheet_name='ZFIvsGL', startrow=len(ZFIvsGLdf) + 2 + len(pivot_tableZFI) + 2)
        result_df.to_excel(writer, sheet_name='ZFIvsGL', startcol=6, index=False)

    print("End Time : ",datetime.datetime.now())
    logging.info(f"Master File Process has Ended on: {datetime.datetime.now()}")
    
except Exception as e:
    ErrorMessage = traceback.extract_tb(e.__traceback__)
    line_number = ErrorMessage[-1][1]
    logging.error(f"Error in File & Folder message: {ErrorMessage} in line {line_number}")
    print(e)
    
