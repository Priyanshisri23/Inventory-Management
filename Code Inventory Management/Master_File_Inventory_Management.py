import pandas as pd
import datetime
import logging
import traceback
import time
from tqdm import tqdm
import Config_File_Inventory_Managment
import File_Folder_Inventory_Managment

print("Start Time : ", datetime.datetime.now())
current_date = datetime.date.today()
Log_date = current_date.strftime("%d%B%Y")

try:
    Log_file_path = fr"{File_Folder_Inventory_Managment.LogFolder}\ProcessLog_{Log_date}.log"
    logging.basicConfig(filename=Log_file_path, level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
    logging.info("Master File Process has been Started")

    # Dictionary to store variable names and corresponding file paths
    file_variable_dict = {}
    # List of file paths and corresponding variable names
    # -------------------------------------HEADER===1---------------------------------------------------------
    # List of file paths and corresponding variable names
    file_variable_mapping = [
        (File_Folder_Inventory_Managment.MB52FilePath, 'MB52Dumpdf'),
        (File_Folder_Inventory_Managment.ZFI_ClosingStockFilePath, 'ZFI_ClosingStockdf'),
        (File_Folder_Inventory_Managment.PriceListFilePath, 'PriceListdf'),
        (File_Folder_Inventory_Managment.BhismaFilePath, 'InHouseBhismaNeedledf'),
        (File_Folder_Inventory_Managment.BhismaFilePath, 'QNPLBhismaNeedledf'),
        (File_Folder_Inventory_Managment.BhismaFilePath, 'SutureBhismaNeedledf'),
        (File_Folder_Inventory_Managment.AgeingMasterFilePath, 'ageing_masterdf'),
        (File_Folder_Inventory_Managment.LastQuarterInventoryFilePath, 'lastQuarterInventorydf'),
    ]

    for file_path, variable_name in tqdm(file_variable_mapping, desc="Reading files"):
        file_variable_dict[variable_name] = pd.read_excel(file_path, header=1)
        print("variable_name----->", variable_name)

    # -----------------------------------------------------HEADER===0----------------------------------------
    # Additional file mappings
    additional_mappings = [
        (File_Folder_Inventory_Managment.mch1_v2_dateFilePath, 'mch1df'),
        (File_Folder_Inventory_Managment.MCHAFilePath, 'mchadf'),
        (File_Folder_Inventory_Managment.SLOC_nonproductivelocationFilePath, 'SLOC_nonproductivelocationdf'),
        (File_Folder_Inventory_Managment.ZFIvsGLFilePath, 'ZFIvsGLdf'),
    ]

    for file_path, variable_name in tqdm(additional_mappings, desc="Reading files"):
        file_variable_dict[variable_name] = pd.read_excel(file_path, header=0)
        print("variable_name----->", variable_name)

    # -----------------------------------------------------HEADER===2----------------------------------------
    additional_mappings = [
        (File_Folder_Inventory_Managment.DivisionSummaryFilePath, 'DivisionSummarydf'),
    ]
    for file_path, variable_name in tqdm(additional_mappings, desc="Reading files"):
        file_variable_dict[variable_name] = pd.read_excel(file_path, header=2)
        print("variable_name----->", variable_name)

    # print("End Time", datetime.datetime.now())

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
    logging.info('Prepared all input files into dataframe')
    print("-------------------------------------------------------------------------------------------------------------------")
    # Column V (Total Stock Quantity) = Sum of column J to O
    MB52Dumpdf['Total Stock Quantity'] = MB52Dumpdf['Unrestricted'] + MB52Dumpdf['Transit and Transfer'] + MB52Dumpdf[
        'Quality Inspection'] + MB52Dumpdf['Restricted-Use Stock'] + MB52Dumpdf['Blocked'] + MB52Dumpdf['Returns']
    # Column W (Total Stock Value) = Sum of column P to U
    MB52Dumpdf['Total Stock Value'] = MB52Dumpdf['Value Unrestricted'] + MB52Dumpdf['Val. in Trans./Tfr'] + MB52Dumpdf[
        'Value in QualInsp.'] + MB52Dumpdf['Value Restricted'] + MB52Dumpdf['Value BlockedStock'] + MB52Dumpdf['Value Rets Blocked']
    # Column X (Rate per Unit) = Column W/V.
    MB52Dumpdf['Rate P.u.'] = MB52Dumpdf["Total Stock Value"] / MB52Dumpdf["Total Stock Quantity"]

    MB52Dumpdf['Plant & SKU'] = MB52Dumpdf.apply(
        lambda row: ''.join(str(col) if pd.notnull(col) else '' for col in [row['Plant'], row['Material']]), axis=1)
    MB52Dumpdf['SKU & Batch'] = MB52Dumpdf.apply(
        lambda row: ''.join(str(col) if pd.notnull(col) else '' for col in [row['Material'], row['Batch']]), axis=1)
    MB52Dumpdf['Plant SKU & Batch'] = MB52Dumpdf.apply(lambda row: ''.join(
        str(col) if pd.notnull(col) else '' for col in [row['Plant'], row['Material'], row['Batch']]), axis=1)
    MB52Dumpdf['Plant SKU Batch & SL'] = MB52Dumpdf.apply(lambda row: ''.join(
        str(col) if pd.notnull(col) else '' for col in
        [row['Plant'], row['Material'], row['Batch'], row['Storage location']]), axis=1)
    MB52Dumpdf['Mat Desc Batch'] = MB52Dumpdf.apply(
        lambda row: ''.join(str(col) if pd.notnull(col) else '' for col in [row['Material description'], row['Batch']]),
        axis=1)

    # mcha1 file concatenation
    mchadf['SKU & Batch'] = mchadf.apply(
        lambda row: ''.join(str(col) if pd.notnull(col) else '' for col in [row['Material'], row['Batch']]), axis=1)

    MB52Dumpdf.insert(29, 'Plant Sku & Val Type', '')
    MB52Dumpdf.insert(30, 'Mat dec & valn type', '')
    MB52Dumpdf.insert(31, 'SKU & Valn Type', '')
    logging.info("Merged the Columns and Created New Column")

    # ==================================Valuation Type=======================================
    # mcha  file   to get the “Valuation Type” column from MCHA file
    merged_data = MB52Dumpdf.merge(mchadf.drop_duplicates(subset='SKU & Batch'), on='SKU & Batch', how='left')
    MB52Dumpdf['Valan type from MCHA(SKU+Batch)'] = merged_data['Valuation Type']
    MB52Dumpdf['Valan type from MCHA(SKU+Batch)'] = MB52Dumpdf['Valan type from MCHA(SKU+Batch)'].fillna('NA')

    # Concatenate
    MB52Dumpdf['Plant Sku & Val Type'] = MB52Dumpdf.apply(lambda row: ''.join(
        str(col) if pd.notnull(col) else '' for col in
        [row['Plant'], row['Material'], row['Valan type from MCHA(SKU+Batch)']]), axis=1)
    MB52Dumpdf['Mat dec & valn type'] = MB52Dumpdf.apply(lambda row: ''.join(
        str(col) if pd.notnull(col) else '' for col in
        [row['Material description'], row['Valan type from MCHA(SKU+Batch)']]), axis=1)
    MB52Dumpdf['SKU & Valn Type'] = MB52Dumpdf.apply(lambda row: ''.join(
        str(col) if pd.notnull(col) else '' for col in [row['Material'], row['Valan type from MCHA(SKU+Batch)']]), axis=1)

    # ===================================ZFI Closing Stock=====================================
    ZFI_ClosingStockdf['Plant SKU & Batch'] = ZFI_ClosingStockdf.apply(lambda row: ''.join(
        str(col) if pd.notnull(col) else '' for col in [row['Plant'], row['Material'], row['Batch']]), axis=1)
    ZFI_ClosingStockdf['SKU & Batch'] = ZFI_ClosingStockdf.apply(
        lambda row: ''.join(str(col) if pd.notnull(col) else '' for col in [row['Material'], row['Batch']]), axis=1)
    ZFI_ClosingStockdf['SKU & Valn Type'] = ZFI_ClosingStockdf.apply(
        lambda row: ''.join(str(col) if pd.notnull(col) else '' for col in [row['Material'], row['Valuation Type']]), axis=1)
    ZFI_ClosingStockdf['Concatenate(Mat desc+Valuation type)'] = ZFI_ClosingStockdf.apply(lambda row: ''.join(
        str(col) if pd.notnull(col) else '' for col in [row['Material description'], row['Valuation Type']]), axis=1)
    ZFI_ClosingStockdf['Rate per Unit'] = ZFI_ClosingStockdf["Total Value"] / ZFI_ClosingStockdf["Total Stock"]

    merged_data = MB52Dumpdf.merge(ZFI_ClosingStockdf.drop_duplicates(subset='Plant SKU & Batch'), on='Plant SKU & Batch', how='left')
    MB52Dumpdf['Rate used Basis'] = 'Plant SKU & Batch'
    MB52Dumpdf['ZFI Rate'] = merged_data['Rate per Unit']

    merged_data = MB52Dumpdf.merge(ZFI_ClosingStockdf.drop_duplicates(subset = 'SKU & Batch'), on = 'SKU & Batch', how = 'left')
    MB52Dumpdf.loc[MB52Dumpdf['ZFI Rate'].isnull(), 'Rate used Basis'] = 'SKU & Batch'
    MB52Dumpdf.loc[MB52Dumpdf['ZFI Rate'].isnull(), 'ZFI Rate'] = merged_data['Rate per Unit']

    merged_data = MB52Dumpdf.merge(ZFI_ClosingStockdf.drop_duplicates(subset = 'SKU & Valn Type'), on ='SKU & Valn Type', how = 'left')
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
        amt_as_per_gl = filtered_df['Amount'].sum() / 10 ** 7

        ZFIfiltered_df = pivot_tableZFI[pivot_tableZFI['Category'] == particular]
        amt_as_per_zfi = ZFIfiltered_df['Total Value'].sum() / 10 ** 7
        Mb52filtered_df = pivot_tableMB52[pivot_tableMB52['Category'] == particular]
        amt_as_per_mb52 = Mb52filtered_df['ZFI Value as of Jun23'].sum() / 10 ** 7
        results.append({'Particulars': particular, 'Amt as per GL': amt_as_per_gl, 'Amt as per ZFI': amt_as_per_zfi,
                        'Amt as per MB52': amt_as_per_mb52})

    result_df = pd.DataFrame(results)
    result_df['ZFI Vs GL'] = result_df['Amt as per GL'] - result_df['Amt as per ZFI']
    result_df['MB52 Vs GL'] = result_df['Amt as per GL'] - result_df['Amt as per MB52']

    # ======================================Under Valuation====================================

    MB52Dumpdf['Plant'] = MB52Dumpdf['Plant'].astype(str)
    filtered_df = MB52Dumpdf[MB52Dumpdf['Plant'].isin(['1012', '1014'])]
    filteredundervaluation_df = filtered_df.loc[:, :'Material Group Desc.']

    merged_dataa = []
    for i, row in filteredundervaluation_df.iterrows():
        PSBrevisedratematched = lastQuarterInventorydf[
            lastQuarterInventorydf['Plant SKU & Batch'] == row['Plant SKU & Batch']]
        SBrevisedratematched = lastQuarterInventorydf[lastQuarterInventorydf['SKU & Batch'] == row['SKU & Batch']]
        SVrevisedratematched = lastQuarterInventorydf[
            lastQuarterInventorydf['SKU & Valn Type'] == row['SKU & Valn Type']]
        Mrevisedratematched = lastQuarterInventorydf[lastQuarterInventorydf['Material'] == row['Material']]
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
    filteredundervaluation_df = pd.DataFrame(merged_dataa)
    filteredundervaluation_df['Revised Value'] = filteredundervaluation_df['Revised Rate'] * filteredundervaluation_df[
        'Total Stock Quantity']
    filteredundervaluation_df['Under valn'] = filteredundervaluation_df['ZFI Value as of Jun23'] - \
                                              filteredundervaluation_df['Revised Value']

    merged_data = MB52Dumpdf.merge(PriceListdf.drop_duplicates(subset='Material Group Desc.'), on='Material Group Desc.', how='left')
    MB52Dumpdf['Over valuation v lookup of price list'] = merged_data['Material Group Desc.'].fillna('NA')
    
    # ======================Overvaluation=================================
    Overvaluationfiltered_df = MB52Dumpdf[~MB52Dumpdf['Over valuation v lookup of price list'].isin(['NA','PAPER', 'Paper'])]
    merged_data = Overvaluationfiltered_df.merge(PriceListdf.drop_duplicates(subset='Material'), on='Material', how='left')
    Overvaluationfiltered_df.loc[:, 'Revised Rate'] = merged_data['Revised Rate'].fillna(Overvaluationfiltered_df['ZFI Rate'])
    Overvaluationfiltered_df.loc[:, 'Revised Value'] = Overvaluationfiltered_df['Revised Rate'] * Overvaluationfiltered_df['Total Stock Quantity']
    Overvaluationfiltered_df.loc[:, 'Overvaluation'] = Overvaluationfiltered_df['ZFI Value as of Jun23'] - Overvaluationfiltered_df['Revised Value']
    # # Use .loc for assignment to avoid SettingWithCopyWarning
    # Overvaluationfiltered_df.loc[:, 'Revised Rate'] = merged_data['Revised Rate']
    # Overvaluationfiltered_df.loc[:, 'Revised Rate'].fillna(Overvaluationfiltered_df['ZFI Rate'], inplace=True)
    # Overvaluationfiltered_df.loc[:, 'Revised Value'] = Overvaluationfiltered_df['Revised Rate'] * Overvaluationfiltered_df['Total Stock Quantity']
    # Overvaluationfiltered_df.loc[:, 'Overvaluation'] = Overvaluationfiltered_df['ZFI Value as of Jun23'] - Overvaluationfiltered_df['Revised Value']

    merged_data = MB52Dumpdf.merge(filteredundervaluation_df.drop_duplicates(subset='Plant SKU Batch & SL'), on='Plant SKU Batch & SL', how='left')
    MB52Dumpdf.loc[:,'Revised Rate'] = merged_data['Revised Rate']
    merged_data = MB52Dumpdf.merge(Overvaluationfiltered_df.drop_duplicates(subset='Plant SKU Batch & SL'), on='Plant SKU Batch & SL', how='left')
    MB52Dumpdf.loc[:,'Revised Rate'] = merged_data['Revised Rate_y']
    
    MB52Dumpdf['ME Value as of Jun 23'] = MB52Dumpdf['Revised Rate'] * MB52Dumpdf['Total Stock Quantity']
    MB52Dumpdf['ME Valn Adj'] = MB52Dumpdf['ME Value as of Jun 23'] - MB52Dumpdf['ZFI Value as of Jun23']
    MB52Dumpdf['Material description & Type'] = MB52Dumpdf.apply(lambda row: ''.join(str(col) if pd.notnull(col) else '' for col in [row['Material Group Desc.'], row['Material type']]), axis=1)
    output_file_path = rf"{Config_File_Inventory_Managment.OutputFolder}\MB52Dump.xlsx"
    with pd.ExcelWriter(output_file_path, engine='openpyxl', mode='w') as writer:
        MB52Dumpdf.to_excel(writer, sheet_name='MB52', index=False)
        ZFI_ClosingStockdf.to_excel(writer, sheet_name='ZFI_Closing_Stock', index=False)
        ZFIvsGLdf.to_excel(writer, sheet_name='ZFIvsGL', index=False)
        pivot_tableZFI.to_excel(writer, sheet_name='ZFIvsGL', startrow=len(ZFIvsGLdf) + 2, index=False)
        pivot_tableMB52.to_excel(writer, sheet_name='ZFIvsGL', startrow=len(ZFIvsGLdf) + 2 + len(pivot_tableZFI) + 2)
        result_df.to_excel(writer, sheet_name='ZFIvsGL', startcol=6, index=False)

    print("End Time : ", datetime.datetime.now())
    logging.info(f"Master File Process has Ended on: {datetime.datetime.now()}")

except Exception as e:
    ErrorMessage = traceback.extract_tb(e.__traceback__)
    line_number = ErrorMessage[-1][1]
    logging.exception(f"Error: {str(e)}")
    logging.error(f"Error in File & Folder message: {str(e)} in line {line_number}")
    print('e', e, 'line_Number', line_number)
