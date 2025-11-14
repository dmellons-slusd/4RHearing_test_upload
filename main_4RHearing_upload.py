from slusdlib import core, aeries
from pandas import DataFrame, read_excel, read_sql_query, concat
from sqlalchemy import text
from typing import Union
from decouple import config
import os
import re
from datetime import datetime

cnxn = aeries.get_aeries_cnxn(access_level='w') if config('ENVIRONMENT', default=None) == 'PROD' else aeries.get_aeries_cnxn(database=config('TEST_DATABASE', default='DST25000SLUSD_DAILY'), access_level='w')
sql = core.build_sql_object()

def read_all_excel_sheets(
    file_path: str,
    existing_df: Union[DataFrame, None] = None
) -> DataFrame:
    """
    Reads all sheets from a multi-tab Excel (.xlsx) file into a single 
    pandas DataFrame. Optionally appends the data to an existing DataFrame.

    Args:
        file_path (str): The full path to the Excel file.
        existing_df (Union[pd.DataFrame, None]): An optional existing DataFrame
            to append the new data to. If None, a new DataFrame is created.

    Returns:
        pd.DataFrame: A single DataFrame containing data from all sheets, 
                      optionally appended to the existing_df.
    """
    
    # 1. Read all sheets into a dictionary of DataFrames
    # sheet_name=None returns a dictionary where keys are sheet names and values are DataFrames
    try:
        all_sheets_data = read_excel(file_path, sheet_name=None)
    except FileNotFoundError:
        core.log(f"Error: The file was not found at path: {file_path}")
        return existing_df if existing_df is not None else DataFrame()
    except Exception as e:
        core.log(f"An error occurred while reading the Excel file: {e}")
        return existing_df if existing_df is not None else DataFrame()

    # 2. Concatenate all DataFrames in the dictionary into a single DataFrame
    # ignore_index=True resets the index of the combined DataFrame
    list_of_dfs = list(all_sheets_data.values())
    combined_df = concat(list_of_dfs, ignore_index=True)
    
    # 3. Append to the existing DataFrame or return the new one
    if existing_df is not None:
        # Concatenate the existing DataFrame with the newly combined data
        appended_df = concat([existing_df, combined_df], ignore_index=True)
        return appended_df
    else:
        return combined_df
def get_next_sq(id, cnxn, table) -> int:
    """Get the next sequence number for a given table."""
    query = f"SELECT top 1 SQ FROM {table} WHERE PID = :id ORDER BY SQ DESC"
    result = read_sql_query(text(query), cnxn, params={"id": id})
    if not result.empty:
        next_sq = result['SQ'].iloc[0] + 1
        return int(next_sq)
    else:
        return 1
def get_grade_from_id(id, cnxn) -> Union[str, None]:
    """Get the grade for a given student ID."""
    query = "SELECT TOP 1 GR FROM STU WHERE DEL = 0 AND TG = '' AND ID = :id"
    result = read_sql_query(text(query), cnxn, params={"id": id})
    if not result.empty:
        grade = result['GR'].iloc[0]
        return str(grade)
    else:
        return None  
def main():
    data: DataFrame = DataFrame()
    for file in os.listdir('./in'):
        if file.endswith('.xlsx'):
            # Extract date from filename (format: m_DD_yy at the end)
            date_pattern = r'(\d{1,2}_\d{2}_\d{2})(?:\.xlsx)?$'
            match = re.search(date_pattern, file)

            if match:
                date_str = match.group(1).replace('_', '/')
                # Parse to datetime for proper date handling
                file_date = datetime.strptime(date_str, '%m/%d/%y')
            else:
                file_date = None
                core.log(f"Warning: Could not extract date from {file}")

            full_path = os.path.join('./in', file)
            temp_df = read_all_excel_sheets(full_path)

            # Add date column
            if file_date:
                temp_df['File_Date'] = file_date

            data = concat([data, temp_df], ignore_index=True)
            core.log(f"Data from {file}:")

    # only process rows where first column is 'P'
    data = data[data.iloc[:, 0] == 'P']
    data.to_csv('out.csv', index=False)
    for index, row in data.iterrows():
        params = {}
        params['PID'] = int(row['Stu ID'])
        params['SQ'] = get_next_sq(params['PID'], cnxn, table='HRN')
        params['GR'] = get_grade_from_id(params['PID'], cnxn)
        params['SR'] = row.iloc[0]
        params['SL'] = row.iloc[0]
        params['PF'] = row.iloc[0]
        params['TD'] = row['File_Date']
        core.log(f'Inserting record: {params}')
        # input("Press Enter to continue...")
        with cnxn.connect() as conn:
            conn.execute(text(sql.INSERT_HRN), params)
            conn.commit()
    core.log(f"Done processing all files.")              
    core.log(f"Total records processed: {len(data)}")              
    core.log("=" * 80)              
if __name__ == "__main__":
    main()