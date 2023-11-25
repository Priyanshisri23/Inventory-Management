import pandas as pd
import datetime
import logging
import traceback
import calendar
from tqdm import tqdm
import Config_File_Inventory_Managment
import File_Folder_Inventory_Managment
import warnings

print("Start Time",datetime.datetime.now())
current_date = datetime.date.today()
Log_date=current_date.strftime("%d%B%Y")

try:
    warnings.filterwarnings('ignore', category=pd.errors.SettingWithCopyWarning)
    log_file_path = fr"{File_Folder_Inventory_Managment.LogFolder}\ProcessLog_{Log_date}.log"
    # print(log_file_path)
    logging.basicConfig(filename=log_file_path, level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
    # print(log_file_path)
    logging.info("Master File Process has been Started")
    
    # Dictionary to store variable names and corresponding file paths
    file_variable_dict = {}
    # List of file paths and corresponding variable names
    # -------------------------------------HEADER===1---------------------------------------------------------
    # List of file paths and corresponding variable names
    file_variable_mapping = [
        (File_Folder_Inventory_Managment.MB52FilePath, 'MB52Dumpdf'),
        # (File_Folder_Inventory_Managment.ZFI_ClosingStockFilePath, 'ZFI_ClosingStockdf'),
        (File_Folder_Inventory_Managment.PriceListFilePath, 'PriceListdf'),
        (File_Folder_Inventory_Managment.BhismaFilePath, 'InHouseBhismaNeedledf'),
        (File_Folder_Inventory_Managment.BhismaFilePath, 'QNPLBhismaNeedledf'),
        (File_Folder_Inventory_Managment.BhismaFilePath, 'SutureBhismaNeedledf'),
        (File_Folder_Inventory_Managment.AgeingMasterFilePath, 'ageing_masterdf'),
        (File_Folder_Inventory_Managment.LastQuarterInventoryFilePath, 'lastQuarterInventorydf'),
        (File_Folder_Inventory_Managment.DivisionSummaryFilePath, 'DivisionSummaryheader1df'),
    ]

    for file_path, variable_name in tqdm(file_variable_mapping, desc="Reading files"):
        file_variable_dict[variable_name] = pd.read_excel(file_path, header=1)
        print("variable_name----->", variable_name)

    # -----------------------------------------------------HEADER===0----------------------------------------
    # Additional file mappings
    additional_mappings = [
        (File_Folder_Inventory_Managment.mch1_v2_dateFilePath, 'mch1df'),
        (File_Folder_Inventory_Managment.ZFI_ClosingStockFilePath, 'ZFI_ClosingStockdf'),
        (File_Folder_Inventory_Managment.MCHAFilePath, 'mchadf'),
        
        (File_Folder_Inventory_Managment.ZFIvsGLFilePath, 'ZFIvsGLdf'),
    ]

    for file_path, variable_name in tqdm(additional_mappings, desc="Reading files"):
        file_variable_dict[variable_name] = pd.read_excel(file_path, header=0)
        print("variable_name----->", variable_name)

    # -----------------------------------------------------HEADER===2----------------------------------------
    additional_mappings = [
        (File_Folder_Inventory_Managment.DivisionSummaryFilePath, 'DivisionSummarydf'),
        (File_Folder_Inventory_Managment.SLOC_nonproductivelocationFilePath, 'SLOC_nonproductivelocationdf'),
    ]
    for file_path, variable_name in tqdm(additional_mappings, desc="Reading files"):
        file_variable_dict[variable_name] = pd.read_excel(file_path, header=2)
        print("variable_name----->", variable_name)

    
    PRcompileddf=pd.read_excel(File_Folder_Inventory_Managment.CompiledPRFilePath,sheet_name=' PR compiled',header=2)
    # print(file_variable_dict)
    MB52Dumpdf = file_variable_dict['MB52Dumpdf']
    logging.info(f"Read the MB52Dumpdf file")
    # MB52Dumpdf = int(MB52Dumpdf)
    # print('type', type(MB52Dumpdf))
    ZFI_ClosingStockdf = file_variable_dict['ZFI_ClosingStockdf']
    logging.info(f"Read the ZFI_ClosingStockdf file")
    # ZFI_ClosingStockdf = int(ZFI_ClosingStockdf)
    # print('type', type(ZFI_ClosingStockdf))
    PriceListdf = file_variable_dict['PriceListdf']
    logging.info(f"Read the PriceListdf file")
    # print('type', type(PriceListdf))
    InHouseBhismaNeedledf = file_variable_dict['InHouseBhismaNeedledf']
    logging.info(f"Read the InHouseBhismaNeedledf file")
    # print('type', type(InHouseBhismaNeedledf))
    QNPLBhismaNeedledf = file_variable_dict['QNPLBhismaNeedledf']
    logging.info(f"Read the QNPLBhismaNeedledf file")
    # print('type', type(QNPLBhismaNeedledf))
    SutureBhismaNeedledf = file_variable_dict['SutureBhismaNeedledf']
    logging.info(f"Read the SutureBhismaNeedledf file")
    # print('type', type(SutureBhismaNeedledf))
    ageing_masterdf = file_variable_dict['ageing_masterdf']
    logging.info(f"Read the ageing_masterdf file")
    # print('type', type(ageing_masterdf))
    mch1df = file_variable_dict['mch1df']
    logging.info(f"Read the mch1df file")
    # print('type', type(mch1df))
    mchadf = file_variable_dict['mchadf']
    logging.info(f"Read the mch1df file")
    # print('type', type(mchadf))
    SLOC_nonproductivelocationdf = file_variable_dict['SLOC_nonproductivelocationdf']
    logging.info(f"Read the SLOC_nonproductivelocationdf file")
    # print('type', type(SLOC_nonproductivelocationdf))
    lastQuarterInventorydf = file_variable_dict['lastQuarterInventorydf']
    logging.info(f"Read the lastQuarterInventorydf file")
    # print('type', type(lastQuarterInventorydf))
    ZFIvsGLdf = file_variable_dict['ZFIvsGLdf']
    logging.info(f"Read the ZFIvsGLdf file")
    # print('type', type(ZFIvsGLdf))
    DivisionSummarydf = file_variable_dict['DivisionSummarydf']
    logging.info(f"Read the DivisionSummarydf file")
    # print('type', type(DivisionSummarydf))
    DivisionSummaryheaderdf = file_variable_dict['DivisionSummaryheader1df']
    logging.info('Prepared all input files into dataframe')
    print("-------------------------------------------------------------------------------------------------------------------")
    logging.info('Prepared all input files into dataframe')
    
    MB52Dumpdf['Plant'] = MB52Dumpdf['Plant'].fillna(0).astype(int)
    # Column V (Total Stock Quantity) = Sum of column J to O 
    MB52Dumpdf['Total Stock Quantity'] = MB52Dumpdf['Unrestricted'] + MB52Dumpdf['Transit and Transfer'] + MB52Dumpdf['Quality Inspection']+ MB52Dumpdf['Restricted-Use Stock']+ MB52Dumpdf['Blocked']+ MB52Dumpdf['Returns']
    # Column W (Total Stock Value) = Sum of column P to U
    MB52Dumpdf['Total Stock Value'] = MB52Dumpdf['Value Unrestricted'] + MB52Dumpdf['Val. in Trans./Tfr'] + MB52Dumpdf['Value in QualInsp.'] + MB52Dumpdf['Value Restricted'] + MB52Dumpdf['Value BlockedStock']+ MB52Dumpdf['Value Rets Blocked']
    # Column X (Rate per Unit) = Column W/V.
    MB52Dumpdf['Rate P.u.']=MB52Dumpdf["Total Stock Value"] / MB52Dumpdf["Total Stock Quantity"]
    
    MB52Dumpdf['Plant & SKU'] = MB52Dumpdf.apply(lambda row: ''.join(str(col) if pd.notnull(col) else '' for col in [row['Plant'], row['Material']]), axis=1)
    MB52Dumpdf['SKU & Batch'] = MB52Dumpdf.apply(lambda row: ''.join(str(col) if pd.notnull(col) else '' for col in [row['Material'], row['Batch']]), axis=1)
    MB52Dumpdf['Plant SKU & Batch'] = MB52Dumpdf.apply(lambda row: ''.join(str(col) if pd.notnull(col) else '' for col in [row['Plant'], row['Material'], row['Batch']]), axis=1)
    MB52Dumpdf['Plant SKU Batch &SL'] = MB52Dumpdf.apply(lambda row: ''.join(str(col) if pd.notnull(col) else '' for col in [row['Plant'], row['Material'], row['Batch'],row['Storage location']]), axis=1)
    MB52Dumpdf['Mat Desc Batch'] = MB52Dumpdf.apply(lambda row: ''.join(str(col) if pd.notnull(col) else '' for col in [row['Material description'], row['Batch']]), axis=1)
    
    # mcha1 file concatenation
    mchadf['SKU & Batch'] = mchadf.apply(lambda row: ''.join(str(col) if pd.notnull(col) else '' for col in [row['Material'], row['Batch']]), axis=1)
    
    MB52Dumpdf.insert(29, 'Plant Sku & Val Type','')
    MB52Dumpdf.insert(30, 'Mat dec & valn type','')
    MB52Dumpdf.insert(31, 'SKU & Valn Type','')
    logging.info("Merged the Columns and Created New Column")
    print("Merged the Columns and Created New Column")

    # ==================================Valuation Type=======================================
    # mcha  file   to get the “Valuation Type” column from MCHA file 
    merged_data = MB52Dumpdf.merge(mchadf.drop_duplicates(subset='SKU & Batch'), on='SKU & Batch', how='left')
    MB52Dumpdf['Valan type from MCHA(SKU+Batch)'] = merged_data['Valuation Type']

    # Concatenate
    MB52Dumpdf['Plant Sku & Val Type'] = MB52Dumpdf.apply(lambda row: ''.join(str(col) if pd.notnull(col) else '' for col in [row['Plant'],row['Material'], row['Valan type from MCHA(SKU+Batch)']]), axis=1)
    MB52Dumpdf['Mat dec & valn type'] = MB52Dumpdf.apply(lambda row: ''.join(str(col) if pd.notnull(col) else '' for col in [row['Material description'], row['Valan type from MCHA(SKU+Batch)']]), axis=1)
    MB52Dumpdf['SKU & Valn Type'] = MB52Dumpdf.apply(lambda row: ''.join(str(col) if pd.notnull(col) else '' for col in [row['Material'], row['Valan type from MCHA(SKU+Batch)']]), axis=1)
    
    MB52Dumpdf['Valan type from MCHA(SKU+Batch)'] = MB52Dumpdf['Valan type from MCHA(SKU+Batch)'].fillna('NA')
    logging.info("Fetch Valuation Type value from MCHA file in MB52 file")
    print("Fetch Valuation Type value from MCHA file in MB52 file")
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
    logging.info("Working for ZFI ClosingStock File")
    print("Working for ZFI ClosingStock File")
    
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
    total_values = result_df.sum()

    total_row = pd.DataFrame({
        'Particulars': ['Total'],
        'Amt as per GL': [total_values['Amt as per GL']],
        'Amt as per ZFI': [total_values['Amt as per ZFI']],
        'Amt as per MB52': [total_values['Amt as per MB52']],
        'ZFI Vs GL': [total_values['ZFI Vs GL']],
        'MB52 Vs GL': [total_values['MB52 Vs GL']]
    })

    # Concatenate the original DataFrame with the total row
    result_df = pd.concat([result_df, total_row], ignore_index=True)
    logging.info("Working for ZFI vs GL Sheet")
    print("Working for ZFI vs GL Sheet")
    # ==========================ZFI vs MB52============================================
    ZFI_Closing_Stock = pd.pivot_table(
    ZFI_ClosingStockdf,
    values=['Total Value', 'Total Stock'],
    index=['Material'],
    aggfunc={'Total Value': 'sum', 'Total Stock': 'sum'}
    )
    ZFI_Closing_Stock.reset_index(inplace=True)
    MB52_totalStock_list = []

    # Iterate through materials in ZFI_Closing_Stock
    for material in ZFI_Closing_Stock['Material']:
        filtered_df = MB52Dumpdf[MB52Dumpdf['Material'] == material]
        MB52_totalStock = filtered_df['Total Stock Qty'].sum()
        MB52_totalStock_list.append(MB52_totalStock)

    # Add MB52_totalStock as a new column to ZFI_Closing_Stock
    ZFI_Closing_Stock['MB52_totalStock'] = MB52_totalStock_list
    ZFI_Closing_Stock['Difference']=ZFI_Closing_Stock['MB52_totalStock']-ZFI_Closing_Stock['Total Stock']
    ZFI_Closing_Stock['Rate/unit']=ZFI_Closing_Stock['Total Value']/ZFI_Closing_Stock['Total Stock']
    
    logging.info("Working for ZFI vs MB52 Sheet")
    print("Working for ZFI vs MB52 Sheet")
    # ======================================Under Valuation====================================
    filtered_df = MB52Dumpdf[MB52Dumpdf['Plant'].isin([1012,1014])]
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
    # Convert the 'Revised Rate' column to numeric type
    filteredundervaluation_df['Revised Rate'] = pd.to_numeric(filteredundervaluation_df['Revised Rate'], errors='coerce')
    filteredundervaluation_df['Revised Value'] = filteredundervaluation_df['Revised Rate'] * filteredundervaluation_df['Total Stock Quantity']
    filteredundervaluation_df['Under valn'] = filteredundervaluation_df['ZFI Value as of Jun23'] - filteredundervaluation_df['Revised Value']
    
    logging.info("Working for Undervaluation Sheet")
    print("Working for Undervaluation Sheet")
    
    merged_data = MB52Dumpdf.merge(PriceListdf.drop_duplicates(subset='Material Group Desc.'), on='Material Group Desc.', how='left')
    MB52Dumpdf['Over valuation v lookup of price list'] = merged_data['Material Group Desc.'].fillna('NA')
    
    # ======================Overvaluation=================================
    Overvaluationfiltered_df = MB52Dumpdf[~MB52Dumpdf['Over valuation v lookup of price list'].isin(['NA','PAPER', 'Paper'])]
    merged_data = Overvaluationfiltered_df.merge(PriceListdf.drop_duplicates(subset='Material'), on='Material', how='left')
    Overvaluationfiltered_df.loc[:, 'Revised Rate'] = merged_data['Revised Rate'].fillna(Overvaluationfiltered_df['ZFI Rate'])
    Overvaluationfiltered_df.loc[:, 'Revised Value'] = Overvaluationfiltered_df['Revised Rate'] * Overvaluationfiltered_df['Total Stock Quantity']
    Overvaluationfiltered_df.loc[:, 'Overvaluation'] = Overvaluationfiltered_df['ZFI Value as of Jun23'] - Overvaluationfiltered_df['Revised Value']
    
    merged_data = MB52Dumpdf.merge(filteredundervaluation_df.drop_duplicates(subset='Plant SKU Batch &SL'), on='Plant SKU Batch &SL', how='left')
    MB52Dumpdf.loc[:,'Revised Rate'] = merged_data['Revised Rate']
    merged_data = MB52Dumpdf.merge(Overvaluationfiltered_df.drop_duplicates(subset='Plant SKU Batch &SL'), on='Plant SKU Batch &SL', how='left')
    MB52Dumpdf.loc[:,'Revised Rate'] = merged_data['Revised Rate_y']
    
    logging.info("Working for Overvaluation Sheet")
    print("Working for Overvaluation Sheet")
    
    MB52Dumpdf['ME Value as of Jun 23'] = MB52Dumpdf['Revised Rate'] * MB52Dumpdf['Total Stock Quantity']
    MB52Dumpdf['ME Valn Adj'] = MB52Dumpdf['ME Value as of Jun 23'] - MB52Dumpdf['ZFI Value as of Jun23']
    # 'Division' column in MB52
    merged_data = MB52Dumpdf.merge(ZFI_ClosingStockdf.drop_duplicates(subset='Plant SKU & Batch'), on='Plant SKU & Batch', how='left')

    MB52Dumpdf['Division'] = merged_data['Division']
    merged_data = MB52Dumpdf.merge(DivisionSummaryheaderdf.drop_duplicates(subset='Division'), on='Division', how='left')
    MB52Dumpdf['Division'] = merged_data['Division.1']

    MB52Dumpdf.loc[MB52Dumpdf['Plant'] == 1050, 'Division'] = 'Sricity'
    MB52Dumpdf.loc[MB52Dumpdf['Plant'] == 1013, 'Division'] = 'Abgel'
    MB52Dumpdf.loc[MB52Dumpdf['Plant'] == 1082, 'Division'] = 'CareNow'
    
    MB52Dumpdf['Material description & Type'] = MB52Dumpdf.apply(lambda row: ''.join(str(col) if pd.notnull(col) else '' for col in [row['Material Group Desc.'], row['Material type']]), axis=1)
    #--------------------------------MB52-----------------------------------------------------
    merged_data = MB52Dumpdf.merge(lastQuarterInventorydf.drop_duplicates(subset='Material'),on='Material', how='left')
    row['Needle Check SLOB (basis Mar/Bhisma file)'] = merged_data["SLOC"]
    
    merged_bhisma_df = pd.concat([InHouseBhismaNeedledf, QNPLBhismaNeedledf, SutureBhismaNeedledf], ignore_index=True)
    merged_data = MB52Dumpdf.merge(merged_bhisma_df.drop_duplicates(subset='Needle Code '),left_on='Material', right_on ='Needle Code ', how='left')
    MB52Dumpdf['Needle check Conc(Bhisma file)'] = merged_data["Remarks "]
    
    
    
    merged_data = MB52Dumpdf.merge(DivisionSummarydf.drop_duplicates(subset='SLoc'), right_on='SLoc', left_on='Storage location', how='left')
    MB52Dumpdf['SLOC'] = merged_data['SLoc']
    MB52Dumpdf['Final Rate'] = MB52Dumpdf['Revised Rate']
    # Create a new column 'Final valn comment' and set default value as an empty string
    MB52Dumpdf['Final Valn Comment'] = ''

    # Update 'Final valn comment' based on the condition
    MB52Dumpdf.loc[MB52Dumpdf['Plant'].isin([1012, 1014]), 'Final Valn Comment'] = 'ME valn'
    MB52Dumpdf.loc[
        ~MB52Dumpdf['Over valuation v lookup of price list'].isin(['PAPER', 'NA']), 'Final Valn Comment'] = 'ME valn'

    # Check if values in 'SLOC' column of dataframe1 are present in 'SLOC' column of dataframe2
    mask = MB52Dumpdf['SLOC'].isin(SLOC_nonproductivelocationdf['Storage location'])

    # Apply the condition based on 'Final Valn Comment'
    # MB52Dumpdf['Final Valn Comment'] = 'Provided For'
    MB52Dumpdf.loc[mask & (MB52Dumpdf['Final Valn Comment'] == 'ME valn'), 'Final Valn Comment'] = 'ME valn (Provided for)'
    MB52Dumpdf.loc[mask & MB52Dumpdf['Final Valn Comment'].isna(), 'Final Valn Comment'] = 'Provided for'
    merged_data = MB52Dumpdf.merge(lastQuarterInventorydf.drop_duplicates(subset='Plant SKU Batch &SL'),
                                   on='Plant SKU Batch &SL', how='left')
    MB52Dumpdf['Mar-23 Comment'] = merged_data['Final Valn Comment_y']
    MB52Dumpdf.loc[MB52Dumpdf['Mar-23 Comment'] == 'Provided for', 'Final Valn Comment'] = 'Provided for'

    MB52Dumpdf['Final Value'] = MB52Dumpdf['Final Rate'] * MB52Dumpdf['Total Stock Quantity']
    MB52Dumpdf['Over-Valuation / (Under-Valuation)'] = MB52Dumpdf['ME Value as of Jun 23'] - MB52Dumpdf['Final Value']

    # =================================Batch datee
    mch1df['SKU & Batch'] = mch1df.apply(lambda row: ''.join(str(col) if pd.notnull(col) else '' for col in [row['Material'], row['Batch']]), axis=1)
    # batch date from mch1 file
    BatchmfgMatched = MB52Dumpdf.merge(mch1df.drop_duplicates(subset='SKU & Batch'), on='SKU & Batch', how='left')
    MB52Dumpdf['Batch mfg date'] = BatchmfgMatched["Date of Manufacture"].fillna(BatchmfgMatched['Created On'])
    MB52Dumpdf['Batch Expiry Date'] = BatchmfgMatched["SLED/BBD"]
    mask1 = BatchmfgMatched['Date of Manufacture'].notna() | BatchmfgMatched['Created On'].notna()
    mask2 = BatchmfgMatched['SLED/BBD'].notna()
    MB52Dumpdf.loc[mask1 | mask2, 'Comment'] = 'MCH1'
    
    BatchFVCMatched = MB52Dumpdf.merge(lastQuarterInventorydf.drop_duplicates(subset='SKU & Batch'),on='SKU & Batch', how='left')
    MB52Dumpdf['Batch mfg date'] = MB52Dumpdf['Batch mfg date'].fillna(BatchFVCMatched['Batch mfg date_y'])
    MB52Dumpdf['Batch Expiry Date'] = MB52Dumpdf['Batch Expiry Date'].fillna(BatchFVCMatched["Batch Expiry Date_y"])
    MB52Dumpdf['Batch mfg date'] = pd.to_datetime(MB52Dumpdf['Batch mfg date'])
    MB52Dumpdf['Batch Expiry Date'] = pd.to_datetime(MB52Dumpdf['Batch Expiry Date'])
    # Calculate the "Expiry Yrs" column
    MB52Dumpdf['Expiry Yrs'] = ((MB52Dumpdf['Batch Expiry Date'] - MB52Dumpdf['Batch mfg date']).dt.days / 365).round(0)
    
    last_day = calendar.monthrange(current_date.year, current_date.month)[1]
    last_day_of_current_month = datetime.datetime(current_date.year, current_date.month, last_day)
    MB52Dumpdf.loc[MB52Dumpdf['Comment'] == "MCH1", '#Days'] = (last_day_of_current_month - MB52Dumpdf['Batch Expiry Date']).dt.days
    
    # ====================================Ageing Bracket======================================= 
    def custom_formula(row):
        if row['#Days'] == '':
            return 'NA'
        elif row['#Days'] < 182:
            return '00-06 Months'
        elif row['#Days'] < 365:
            return '06-12 Months'
        elif row['#Days'] < 547:
            return '12-18 Months'
        elif row['#Days'] < 730:
            return '18-24 Months'
        elif row['#Days'] < 1095:
            return '24-36 Months'
        elif row['#Days'] < 1460:
            return '36-48 Months'
        elif row['#Days'] < 1825:
            return '48-60 Months'
        else:
            return '60+ months'
    MB52Dumpdf['Ageing Bracket'] = MB52Dumpdf.apply(custom_formula, axis=1)

    # =================================SKU Group===================================
    def custom_formula(row):
        if row['Ageing Bracket'] == "NA":
            return "NA"
        else:
            try:
                value1 = ageing_masterdf.loc[ageing_masterdf['Material'] == row['Ageing Bracket'], 'Group'].values[0]
            except IndexError:
                value1 = None

            try:
                value2 = ageing_masterdf.loc[ageing_masterdf['Material Group Desc & Material type'] == row['Material description & Type'], 'Group'].values[0]
            except IndexError:
                value2 = None

            return value1 if value1 is not None else value2
    MB52Dumpdf['SKU Group'] = MB52Dumpdf.apply(custom_formula, axis=1)


    # 
    MB52Dumpdf['Ageing Group'] = MB52Dumpdf.apply(lambda row: ''.join(str(col) if pd.notnull(col) else '' for col in [row['SKU Group'], row['Ageing Bracket']]), axis=1)
    MB52Dumpdf.loc[MB52Dumpdf['Ageing Bracket'] == 'NA', 'Ageing Group'] = 'NA'
    # ====================Ageing Value=================================================

    def AgeingValuecustom_formula(row):
        if row['Ageing Group'] == 'NA':
            return 0
        else:
            matching_row = ageing_masterdf[ageing_masterdf['Vlookup'] == row['Ageing Group']]
            if not matching_row.empty:
                index_value = matching_row['% Ageing Provision'].iloc[0]
                return row['Final Value'] * index_value
            else:
                return 0

    # Apply the custom function to your DataFrame
    MB52Dumpdf['Ageing Value'] = MB52Dumpdf.apply(AgeingValuecustom_formula, axis=1)


    # =================================After Ageing Value as on 30 Jun23=================================

    MB52Dumpdf['After Ageing Value as on 30 Jun 23'] = MB52Dumpdf.apply(lambda row: 0 if row['Ageing Group'] == 'NA' else (row['Final Value'] - row['Ageing Value']), axis=1)
    
    PRcompileddf['SKU & Batch'] = PRcompileddf.apply(lambda row: ''.join(str(col) if pd.notnull(col) else '' for col in [row['Material'], row['Batch']]), axis=1)
    # 'Inter co (SKU+Batch) - QNPL+Q Close+Carenow' column 
    merged_data = MB52Dumpdf.merge(PRcompileddf.drop_duplicates(subset='SKU & Batch'), on='SKU & Batch', how='left')
    MB52Dumpdf['Inter co (SKU+Batch) - QNPL+Q Close+Carenow'] = merged_data['Name']
    merged_data = MB52Dumpdf.merge(PRcompileddf.drop_duplicates(subset='Material'), on='Material', how='left')

    MB52Dumpdf['UoM Pur reg'] = merged_data['UOM']
    MB52Dumpdf['UoM check']=MB52Dumpdf.apply(lambda row: 0 if row['UoM Pur reg'] == row['Base Unit of Measure'] else 1, axis=1)
    print("excel operation end Time",datetime.datetime.now())
    output_file_path = rf"{Config_File_Inventory_Managment.OutputFolder}\MB52Dump.xlsx"
    with pd.ExcelWriter(output_file_path, engine='openpyxl', mode='w') as writer:
        MB52Dumpdf.to_excel(writer,sheet_name='MB52', index=False)
        ZFI_ClosingStockdf.to_excel(writer,sheet_name='ZFI_Closing_Stock', index=False)
        ZFI_Closing_Stock.to_excel(writer,sheet_name='ZFI vs MB52', index=False)
        ZFIvsGLdf.to_excel(writer, sheet_name='ZFIvsGL', index=False)
        pivot_tableZFI.to_excel(writer, sheet_name='ZFIvsGL', startrow=len(ZFIvsGLdf) + 4, index=False)
        pivot_tableMB52.to_excel(writer, sheet_name='ZFIvsGL', startrow=len(ZFIvsGLdf) + 4 + len(pivot_tableZFI) + 4)
        result_df.to_excel(writer, sheet_name='ZFIvsGL', startcol=6, index=False)
        filteredundervaluation_df.to_excel(writer, sheet_name='Undervaluation', index=False)
        Overvaluationfiltered_df.to_excel(writer, sheet_name='Overvaluation', index=False)
    print("End Time",datetime.datetime.now())
    logging.info(f"Master File Process has Ended on: {datetime.datetime.now()}")
except Exception as e:
    # print(e)
    ErrorMessage = traceback.extract_tb(e.__traceback__)
    line_number = ErrorMessage[-1][1]
    logging.error(f"Error in File & Folder message: {ErrorMessage} in line {line_number}")
    print(e)
