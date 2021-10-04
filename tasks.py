import boto3
from io import BytesIO
import pandas as pd
from config import connection_details
from db import engine
from queries import SQLQueries as queries
import os
import datetime


def extract_data_from_source(**kwargs):

    # S3 Source connection details
    aws_access_key_id = connection_details['source_details']['aws_access_key_id']
    aws_secret_access_key = connection_details['source_details']['aws_secret_access_key']
    bucket = connection_details['source_details']['bucket']
    s3_file = connection_details['source_details']['s3_file']
    region_name = connection_details['source_details']['region']

    s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key, region_name=region_name)
    file = s3.get_object(Bucket=bucket, Key=s3_file)
    chunk_size = 5000
    first_chunk = True
    file_path = os.getcwd() + '/data.csv'
    print('Loading data.............')
    total_rows = 0
    for chunk in pd.read_csv(BytesIO(file['Body'].read()), chunksize=chunk_size):
        chunk = chunk[['SeriousDlqin2yrs', 'RevolvingUtilizationOfUnsecuredLines',
        'age', 'NumberOfTime30-59DaysPastDueNotWorse', 'DebtRatio', 'MonthlyIncome', 'NumberOfOpenCreditLinesAndLoans',
        'NumberOfTimes90DaysLate', 'NumberRealEstateLoansOrLines', 'NumberOfTime60-89DaysPastDueNotWorse',
        'NumberOfDependents']]
        if first_chunk:
            chunk.to_csv(file_path, header=True, index=False)
            first_chunk = False
        else:
            chunk.to_csv(file_path, header=False, mode='a', index=False)
        total_rows += len(chunk)
        print('Loaded rows:', total_rows)

    if total_rows == 0:
        print('Data is not available..............')
        return ''

    print('Data has been extracted from s3..............')
    # Can add further processing within this step if required


def load_data_into_target(**kwargs):

    # Database Target Connection Details
    host = connection_details['target_details']['host']
    user = connection_details['target_details']['user']
    password = connection_details['target_details']['password']
    database = connection_details['target_details']['database']
    port = connection_details['target_details']['port']

    # S3 Connection Details
    aws_access_key_id = connection_details['source_details']['aws_access_key_id']
    aws_secret_access_key = connection_details['source_details']['aws_secret_access_key']
    bucket = connection_details['source_details']['bucket']
    region = connection_details['source_details']['region']

    file_path = os.getcwd() + '/data.csv'
    load_query = queries().load_data_query.format(bucket_name=bucket, file_name='processed/{}/data.csv'.format(str(datetime.date.today())), access_key=aws_access_key_id, secret_key=aws_secret_access_key, region=region)

    sql_engine = engine(host, user, password, database, port)
    print('pushing processed data into s3')
    s3 = boto3.resource('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
    s3.Object(bucket, 'processed/{}/data.csv'.format(str(datetime.date.today()))).put(Body=open(file_path, 'rb'))
    print('Data pushed to s3')

    connection = sql_engine.raw_connection()
    print('connected')

    cursor = connection.cursor()

    print('creating staging table stg_loan_application_details')
    print(queries().create_staging_table_query)
    cursor.execute(queries().create_staging_table_query)
    print('created table loan_application_details')
    connection.commit()

    print('creating new table loan_application_details if it does not exist')
    print(queries().create_table_query)
    cursor.execute(queries().create_table_query)
    print('created table loan_application_details')
    connection.commit()

    print('Loading data into staging table..........')
    print(load_query)
    cursor.execute(load_query)
    print('Data loaded into staging table...........')
    connection.commit()

    print('Delting data from main table..........')
    print(queries().deactivate_existing_records)
    cursor.execute(queries().deactivate_existing_records)
    print('Data deleted...........')
    connection.commit()

    print('Insert data into main table..........')
    print(queries().insert_into_main)
    cursor.execute(queries().insert_into_main)
    print('Data inserted...........')
    connection.commit()

    cursor.close()


if __name__ == '__main__':
    extract_data_from_source()
    load_data_into_target()
