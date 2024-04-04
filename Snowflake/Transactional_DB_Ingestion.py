import os 
import pandas as pd
import numpy as np
from pathlib import Path
import pymssql
from sqlalchemy import create_engine

# Set the working directory
downloads_path = str(Path.home() / "Downloads")

os.chdir(downloads_path)

# Your existing connection parameters
host = 'stairway.usu.edu'
user = 'dw_group3'
password = 'Supersquids94!'
database = 'dw_group3'

# conn = pymssql.connect(server=host, user=user, password=password, database=database, autocommit=True)
# Create the connection string for SQL Server
conn_str = f'mssql+pymssql://{user}:{password}@{host}/{database}'

# Create the engine
engine = create_engine(conn_str)

# Read the Excel file
excel_file = 'Raw_SamsSubsSandwichConsumption.xlsx'
sheets = pd.read_excel(excel_file, sheet_name=None)

# Create a list of dictionaries to store sheet name and dataframe
dataframes = []

# Iterate over each sheet and create a dataframe
for sheet_name, sheet_data in sheets.items():
    df = pd.DataFrame(sheet_data)
    dataframes.append({'sheet_name': sheet_name, 'dataframe': df})
    print(df.head(10).to_string())
    print('\n')

print('===========================================================')

# Print all sheet names
for df_dict in dataframes:
    print(df_dict['sheet_name'])

print('\n')
print('===========================================================')

try:
    for df_dict in dataframes:
        # if df_dict['sheet_name'] == 'Order':
        #     continue
        print(df_dict['sheet_name'])
        df_dict['dataframe'].to_sql(name=df_dict['sheet_name'], con=engine, if_exists='replace', index=False)
except pymssql._mssql.MSSQLDatabaseException as e:
    print(f"SQL Server Error: {e}")
    print(f"SQL Server Messages: {engine.messages}")