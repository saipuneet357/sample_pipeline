import boto3
from io import BytesIO
import pandas as pd
from config import connection_details


def extract_data_from_source():

    # Source connection details
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
    pass


if __name__ == '__main__':
    file_path = extract_data_from_source()
    if file_path != '':
        load_data_into_target(file_path)
# import pandas as pd
# step 1: connect to google drive and fetch data
# step 2: preprocess your data using mappings
# step 3: load your preprocessed data into a data storage (postgresql, drive, etc...)
# Assumption 1 the data will be fetched from google drive periodically
