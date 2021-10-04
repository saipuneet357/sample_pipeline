# Connection details of source and target machines
# Change connection details to your respective machines
# Using Redshift as the data store and s3 as the source

connection_details = {
    'source_details': {
        'aws_access_key_id': 'AKIA5EGMOHVGHSO2KL7N',
        'aws_secret_access_key': 'EDhqPqv/jln6XcSF8hhI2hEVs64lg4fk/uyacQRv',
        'bucket': 'spuneet-pipeline',
        's3_file': 'sample_data.csv',
        'region': 'ap-south-1'
    },
    
    'target_details': {
        'host': 'localhost',
        'port': 5439,
        'user': 'admin',
        'password': 'password',
        'database': 'dwh'
    }


}
