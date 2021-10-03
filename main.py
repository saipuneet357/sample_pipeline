import boto3
from io import BytesIO
import pandas as pd
from config import connection_details
from db import engine
from queries import SQLQueries as queries


def extract_data_from_source():

    # S3 Source connection details
    aws_access_key_id = connection_details['source_details']['aws_access_key_id']
    aws_secret_access_key = connection_details['source_details']['aws_secret_access_key']
    bucket = connection_details['source_details']['bucket']
    s3_file = connection_details['source_details']['s3_file']
    region_name = connection_details['source_details']['region_name']

    s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key, region_name=region_name)
    file = s3.get_object(Bucket=bucket, Key=s3_file)
    chunk_size = 5000
    first_chunk = True
    file_path = 'data.csv'
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
    return file_path


def load_data_into_target(file_path):
    # Database Target Connection Details
    host = connection_details['target_details']['host']
    user = connection_details['target_details']['user']
    password = connection_details['target_details']['password']
    database = connection_details['target_details']['database']
    port = connection_details['target_details']['port']
    sql_engine = engine(host, user, password, database, port)

    connection = sql_engine.raw_connection()
    print('connected')

    cursor = connection.cursor()
    print('creating new table loan_application_details if it does not exist')
    print(queries().create_table_query)
    cursor.execute(queries().create_table_query)
    print('created table load_application_details')

    print('Loading data into table..........')
    print(queries().load_data_query)
    cursor.execute(queries().load_data_query)
    print('Data loaded into table...........')

    cursor.close()


if __name__ == '__main__':
    # file_path = extract_data_from_source()
    file_path = 'dsaf'
    if file_path != '':
        load_data_into_target(file_path)
