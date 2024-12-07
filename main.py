# Databricks notebook source
# MAGIC %md
# MAGIC # BRONZE - dynamic auto ingest
# MAGIC
# MAGIC **Requirements**
# MAGIC - A function that accept an arugments
# MAGIC - The function will anlyze if the given type of format is valid
# MAGIC

# COMMAND ----------

# import
import os
import pandas as pd

# COMMAND ----------

current_directory = os.getcwd()
files = os.listdir('.')

print(f'Your current directory is: {current_directory}')
for file in files:
    print(file)

# COMMAND ----------

def determine_file_format(file_path):
    file_extension = os.path.splitext(file_path)[-1].lower()
    print(f'{file_path}')

    #file formats
    structured_formats = ['.csv', '.xls', '.xlsx', '.sql', '.parquet', '.orc', '.json']
    semi_structured_formats = ['.json', '.xml', '.yaml', '.html', '.avro']
    unstructured_formats = ['.txt', '.jpeg', '.jpg', '.png', '.gif', '.mp3', '.wav', '.mp4', '.avi', '.pdf']

    if file_extension in structured_formats:
        print(f'INFO: FILE EXTENSION {file} {file_extension} Structured')
        return file_path
    elif file_extension in semi_structured_formats:
        print(f'INFO: FILE EXTENSION {file} {file_extension} Semi-Structured')
        return file_path
    elif file_extension in unstructured_formats:
        print(f'INFO: Unstructured file')
        return file_path
    else:
        print('ERROR: Unknown Format')
        return file_path

structure = 'structured-data'
file_name = 'employee.csv'
file_path = f'/Workspace/Users/labuser8667937_1733537801@vocareum.com/DYNAMIC_BRONZE_INGEST/{structure}/{file_name}'

isValid_file_type = determine_file_format(file_path)

# COMMAND ----------

df = pd.read_csv(isValid_file_type)

df.head()
df.describe()

df[
    ['first_name', 'company_name']
].query('first_name == "John"')

