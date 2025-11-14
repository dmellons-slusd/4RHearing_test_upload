# 4RHearing Data Upload Script

Python script for processing and uploading 4RHearing student roster data to the Aeries database.

## Overview

This script processes multi-tab Excel files containing student hearing screening data, extracts relevant information, and uploads it to the Aeries HRN (Hearing) table.

## Features

- Reads multiple Excel files from the `./in` directory
- Processes all sheets within each Excel file
- Extracts the screening date from the filename (format: `m_DD_yy`)
- Filters records where the first column equals 'P' (Pass)
- Retrieves student grade information from the database
- Generates sequential sequence numbers (SQ) for each student
- Inserts records into the Aeries HRN table

## Requirements

- Python 3.x
- pandas
- sqlalchemy
- python-decouple
- slusdlib (internal library)

## Setup

1. Create a `.env` file with the following variables:
   ```
   ENVIRONMENT=PROD  # or leave unset for test environment
   TEST_DATABASE=DST25000SLUSD_DAILY  # if using test environment
   ```

2. Place Excel files in the `./in` directory
   - Files should be named with the format: `School Rosters 4RHearing 25-26 as of M_DD_YY.xlsx`
   - Example: `Bancroft Rosters 4RHearing 25-26 as of 9_18_25.xlsx`

## Usage

```bash
python main_4RHearing_upload.py
```

The script will:
1. Read all `.xlsx` files from the `./in` directory
2. Extract the date from each filename
3. Process all sheets in each file
4. Add a `File_Date` column with the extracted date
5. Filter for records where the first column is 'P'
6. Export processed data to `out.csv`
7. Upload each record to the Aeries HRN table

## Database Schema

The script inserts data into the HRN table with the following fields:
- `PID` - Student ID
- `SQ` - Sequence number (auto-generated)
- `GR` - Grade (retrieved from STU table)
- `SR` - Screen Result (from first column)
- `SL` - Screen Left (from first column)
- `PF` - Pass/Fail (from first column)
- `TD` - Test Date (from filename)

## Safety Features

- Connects to test database by default (unless `ENVIRONMENT=PROD`)
- Commits are commented out by default in development
- Input validation for student IDs
- Error handling for missing files or grades

## File Structure

```
.
├── main_4RHearing_upload.py   # Main script
├── SQL/
│   ├── HRN_TEST.sql          # Test query
│   └── INSERT_HRN.sql        # Insert statement
├── in/                        # Input Excel files (not tracked)
├── out.csv                    # Output CSV (not tracked)
├── .env                       # Environment config (not tracked)
└── README.md
```

## Notes

- All student data files (`.xlsx`, `.csv`) are excluded from version control
- The `.env` file containing database credentials is excluded from version control
- Always test with the test database before running in production
