import dateparser
from slusdlib import core, aeries
from pandas import DataFrame, read_csv, read_excel, read_sql_query, concat, notna
from sqlalchemy import text
from typing import Union
from decouple import config
import os

cnxn = aeries.get_aeries_cnxn(access_level='w') if config('ENVIRONMENT', default=None) == 'PROD' else aeries.get_aeries_cnxn(database=config('TEST_DATABASE', default='DST25000SLUSD_DAILY'), access_level='w')
sql = core.build_sql_object()

def read_all_excel_sheets_standardized(file_path: str) -> DataFrame:
    """
    Reads all sheets from an Excel file, taking only the first 10 columns (A-J),
    stopping at rows where column A is blank, and applying standardized column names.
    Handles both elementary school format (with Grade) and middle school format (with Period).
    
    Args:
        file_path (str): The full path to the Excel file.
    
    Returns:
        pd.DataFrame: A single DataFrame with standardized columns from all sheets.
    """
    # Sheet names to skip (these are typically summary/aggregate sheets)
    SKIP_SHEETS = ['all', 'address', 'summary', 'total', 'roster']
    
    try:
        # Read all sheets
        all_sheets_data = read_excel(file_path, sheet_name=None)
    except FileNotFoundError:
        core.log(f"Error: The file was not found at path: {file_path}")
        return DataFrame()
    except Exception as e:
        core.log(f"An error occurred while reading the Excel file: {e}")
        return DataFrame()

    list_of_dfs = []
    
    # Detect school type by checking first classroom sheet
    school_type = None
    for sheet_name, df in all_sheets_data.items():
        if any(skip in sheet_name.lower() for skip in SKIP_SHEETS):
            continue
        # Check column names to determine school type
        cols_str = ' '.join([str(col).lower() for col in df.columns[:10]])
        if 'period' in cols_str or 'course' in cols_str:
            school_type = 'MIDDLE'
            core.log("Detected MIDDLE SCHOOL format (with Period/Course)")
        else:
            school_type = 'ELEMENTARY'
            core.log("Detected ELEMENTARY format (with Grade)")
        break
    
    for sheet_name, df in all_sheets_data.items():
        # Skip summary/aggregate sheets
        if any(skip in sheet_name.lower() for skip in SKIP_SHEETS):
            core.log(f"Skipping summary sheet: {sheet_name}")
            continue
        
        # Get the number of columns available (up to 10)
        num_cols = min(df.shape[1], 10)
        
        # Take only the available columns (up to 10)
        df = df.iloc[:, :num_cols].copy()
        
        # Apply standardized column names based on school type
        if school_type == 'MIDDLE':
            # Middle school: A-J = Status, Last, First, Seat, ID, DOB, Teacher, Period, Course, Gender
            middle_cols = [
                'Status',           # Column A: P/NP/Abs/CNC
                'Last_Name',        # Column B
                'First_Name',       # Column C
                'Seat_Number',      # Column D
                'Student_ID',       # Column E
                'DOB',              # Column F
                'Teacher_Name',     # Column G
                'Period',           # Column H
                'Course_Title',     # Column I
                'Gender'            # Column J
            ]
            df.columns = middle_cols[:num_cols]
            # Add Grade column as None for middle school (they don't have grades in the file)
            df['Grade'] = None
            # Add SPED as None (not in first 10 columns for middle school)
            df['SPED'] = None
        else:
            # Elementary: A-J = Status, Last, First, Seat, ID, Grade, Gender, DOB, Teacher, SPED
            elem_cols = [
                'Status',           # Column A
                'Last_Name',        # Column B
                'First_Name',       # Column C
                'Seat_Number',      # Column D
                'Student_ID',       # Column E
                'Grade',            # Column F
                'Gender',           # Column G
                'DOB',              # Column H
                'Teacher_Name',     # Column I
                'SPED'              # Column J
            ]
            df.columns = elem_cols[:num_cols]
        
        # Ensure we have at least the Status column
        if 'Status' not in df.columns:
            core.log(f"Warning: Sheet '{sheet_name}' has no data columns, skipping")
            continue
        
        # Filter: keep only rows where Status (column A) is not blank
        df = df[notna(df['Status']) & (df['Status'].astype(str).str.strip() != '')]
        
        # Additional validation: Status should typically be P, NP, Abs, or CNC
        # Skip sheets where Status contains unusual values (like school names)
        status_values = df['Status'].unique()
        valid_statuses = ['P', 'NP', 'Abs', 'CNC', 'abs', 'cnc', 'np', 'P ', 'NP ']
        if len(status_values) > 0 and not any(val.strip() if isinstance(val, str) else val in valid_statuses for val in status_values):
            core.log(f"Skipping sheet '{sheet_name}' - appears to be a summary sheet (Status values: {status_values[:5]})")
            continue
        
        # Add sheet name for reference
        df['Sheet_Name'] = sheet_name
        
        core.log(f"Processed sheet '{sheet_name}': {len(df)} rows")
        list_of_dfs.append(df)
    
    # Concatenate all sheets
    if list_of_dfs:
        combined_df = concat(list_of_dfs, ignore_index=True)
        return combined_df
    else:
        return DataFrame()

def get_next_sq(id, cnxn, table) -> int:
    """Get the next sequence number for a given table."""
    query = f"SELECT top 1 SQ FROM {table} WHERE PID = :id ORDER BY SQ DESC"
    result = read_sql_query(text(query), cnxn, params={"id": id})
    if not result.empty:
        next_sq = result['SQ'].iloc[0] + 1
        return int(next_sq)
    else:
        return 1

def get_grade_from_id(id:int, cnxn) -> Union[str, None]:
    """Get the grade for a given student ID."""
    id_override_dict = {
        113507: '8',
    }
    if id in id_override_dict.keys():
        core.log(f'Overriding grade for ID {id} to {id_override_dict[id]}')
        return id_override_dict[id]
    query = "SELECT TOP 1 GR FROM STU WHERE DEL = 0 AND TG = '' AND ID = :id"
    result = read_sql_query(text(query), cnxn, params={"id": id})
    if not result.empty:
        grade = result['GR'].iloc[0]
        return str(grade)
    else:
        return None

def check_duplicate_exists(pid: int, test_date, cnxn) -> bool:
    """Check if a record already exists for the given PID and test date."""
    query = "SELECT COUNT(*) as cnt FROM HRN WHERE PID = :pid AND TD = :test_date"
    result = read_sql_query(text(query), cnxn, params={"pid": pid, "test_date": test_date})
    if not result.empty and result['cnt'].iloc[0] > 0:
        return True
    return False

def main():
    data: DataFrame = DataFrame()
    
    # Read and clean nurse info CSV
    nurse_info = read_csv('nurses_and_dates.csv')
    nurse_info.columns = nurse_info.columns.str.strip()
    
    core.log("Starting to process Excel files...")
    
    for file in os.listdir('./in'):
        if not file.endswith('.xlsx'): 
            continue
        
        # Extract school name from filename
        school_name = file.split(' ')[0]
        
        # Find matching nurse info
        school_nurse_info = nurse_info[nurse_info['school'].str.contains(school_name, case=False, na=False)]
        
        if not school_nurse_info.empty:
            date_str = school_nurse_info['date'].iloc[0]
            file_date = dateparser.parse(date_str)
            nurse = f"{school_nurse_info['nurse_first'].iloc[0]} {school_nurse_info['nurse_last'].iloc[0]}"
            sc_code = school_nurse_info['sc'].iloc[0]
            core.log(f"Processing {file}: Nurse={nurse}, Date={file_date}, SC={sc_code}")
        else:
            file_date = None
            nurse = None
            sc_code = None
            core.log(f"Warning: Could not find nurse info for {school_name}")
        
        # Read Excel file with standardized columns
        full_path = os.path.join('./in', file)
        temp_df = read_all_excel_sheets_standardized(full_path)
        
        if temp_df.empty:
            core.log(f"Warning: No data extracted from {file}")
            continue
        
        # Add metadata columns
        temp_df['SC'] = sc_code
        temp_df['File_Date'] = file_date
        temp_df['Nurse'] = nurse
        temp_df['School_Name'] = school_name
        temp_df['Initials'] = ''.join([word[0] for word in nurse.split()]) if notna(nurse) and nurse else None
        
        core.log(f"Added {len(temp_df)} rows from {file}")
        
        # Concatenate to main dataframe
        data = concat([data, temp_df], ignore_index=True)
    
    core.log(f"Total rows before filtering: {len(data)}")

    # Strip whitespace from Status column and filter for only 'P' status rows
    if not data.empty:
        data['Status'] = data['Status'].astype(str).str.strip().str.upper()
        data = data[data['Status'] == 'P']
        core.log(f"Rows after filtering for 'P': {len(data)}")
    
    # CRITICAL: Save and remove rows with missing Student_ID
    initial_count = len(data)
    missing_id_rows = data[~notna(data['Student_ID']) | (data['Student_ID'] == '')]
    data = data[notna(data['Student_ID']) & (data['Student_ID'] != '')]
    removed_count = initial_count - len(data)
    
    if removed_count > 0:
        core.log(f"WARNING: Found {removed_count} rows with missing Student_ID")
        # Save missing ID rows to CSV
        missing_id_rows.to_csv('missing_ids.csv', index=False)
        core.log(f"Saved rows with missing Student_IDs to 'missing_ids.csv'")
    
    # Convert Grade to integer format
    def convert_grade_to_int(grade):
        """Convert grade values to integers. K=0, TK=-1, PS=-2"""
        if not notna(grade) or grade is None:
            return None
        
        # Convert to string and strip whitespace
        grade_str = str(grade).strip().upper()
        
        # Handle special cases
        if grade_str == 'K':
            return 0
        if grade_str in ['TK', 'T-K', 'T K']:
            return -1
        if grade_str in ['PS', 'P-S', 'P S', 'PRESCHOOL']:
            return -2
        
        # Try to convert to integer
        try:
            # Handle float strings like "5.0"
            grade_float = float(grade_str)
            return int(grade_float)
        except (ValueError, TypeError):
            core.log(f"WARNING: Could not convert grade '{grade}' to integer, returning None")
            return None
    
    # Apply grade conversion
    data['Grade'] = data['Grade'].apply(convert_grade_to_int)
    core.log(f"Converted grades to integer format (K=0, TK=-1, PS=-2)")
    
    core.log(f"Final row count for processing: {len(data)}")
    
    # Save to CSV
    data.to_csv('out.csv', index=False)
    core.log(f"Saved output to out.csv with {len(data)} rows and {len(data.columns)} columns")
    core.log(f"Columns: {data.columns.tolist()}")
    
    # Upload to database if enabled
    if upload := config('UPLOAD', default='False', cast=bool):
        print(f'{upload = }')
        skipped_count = 0
        error_count = 0
        success_count = 0
        duplicate_count = 0

        for index, row in data.iterrows():
            # Safety check: Skip rows with missing Student_ID
            if not notna(row['Student_ID']) or row['Student_ID'] == '':
                core.log(f"Skipping row {index} - missing Student_ID")
                skipped_count += 1
                continue
            
            params = {}
            try:
                params['PID'] = int(row['Student_ID'])
            except (ValueError, TypeError) as e:
                core.log(f"ERROR: Invalid Student_ID '{row['Student_ID']}' at row {index}: {e}")
                error_count += 1
                continue

            # Check for duplicate before proceeding
            if check_duplicate_exists(params['PID'], row['File_Date'], cnxn):
                core.log(f"DUPLICATE: Student {params['PID']} ({row['First_Name']} {row['Last_Name']}) already has a record for date {row['File_Date']} - skipping")
                duplicate_count += 1
                continue

            params['SQ'] = get_next_sq(params['PID'], cnxn, table='HRN')
            
            # FIXED: Handle grade properly (already converted to int in preprocessing)
            # First, try to use grade from the Excel file (already converted to int)
            if notna(row['Grade']) and row['Grade'] is not None:
                params['GR'] = int(row['Grade'])
            else:
                # For middle schools or missing grades, query from database
                db_grade = get_grade_from_id(params['PID'], cnxn)
                if db_grade is not None:
                    params['GR'] = int(db_grade)
                else:
                    # Last resort: Skip this record
                    core.log(f"WARNING: No grade found for student {params['PID']} ({row['First_Name']} {row['Last_Name']}), skipping record")
                    skipped_count += 1
                    continue  # Skip this student
            
            params['SR'] = row['Status']
            params['SL'] = row['Status']
            params['PF'] = row['Status']
            params['TD'] = row['File_Date']
            params['SCL'] = row['SC']
            params['IN'] = '4RH' #row['Initials'] if notna(row.get('Initials')) else None
            
            core.log(f'Inserting record: {params}')
            
            try:
                with cnxn.connect() as conn:
                    conn.execute(text(sql.INSERT_HRN), params)
                    conn.commit()
                success_count += 1
            except Exception as e:
                core.log(f"ERROR inserting student {params['PID']} ({row['First_Name']} {row['Last_Name']}): {e}")
                error_count += 1
                continue  # Continue with next student
        
        core.log(f"Done processing all files.")
        core.log(f"Successfully inserted: {success_count}")
        core.log(f"Skipped (no grade): {skipped_count}")
        core.log(f"Duplicates (already uploaded): {duplicate_count}")
        core.log(f"Errors: {error_count}")
        core.log(f"Total records processed: {len(data)}")
        core.log("=" * 80)


if __name__ == "__main__":
    main()