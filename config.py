connection_details = {
    'source_details': {
        'aws_access_key_id': 'AKIA5EGMOHVGHSO2KL7N',
        'aws_secret_access_key': 'EDhqPqv/jln6XcSF8hhI2hEVs64lg4fk/uyacQRv',
        'bucket': 'spuneet-pipeline',
        's3_file': 'sample_data.csv',
        'region_name': 'ap-south-1'
    },

    'target_details': {
        'host': 'localhost',
        'port': 3307,
        'user': 'root',
        'password': 'password',
        'database': 'samplepipeline'
    }


}

print(connection_details)
