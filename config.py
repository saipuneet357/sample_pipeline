connection_details = {
    'source_details': {
        'aws_access_key_id': 'AKIA5EGMOHVGHSO2KL7N',
        'aws_secret_access_key': 'EDhqPqv/jln6XcSF8hhI2hEVs64lg4fk/uyacQRv',
        'bucket': 'spuneet-pipeline',
        's3_file': 'sample_data.csv',
        'region_name': 'ap-south-1'
    },

    'target_details': {
        'host': '127.0.0.1',
        'database': 'datawarehouse',
        'user': 'root',
        'password': 'root',
        'port': '5432'
    }


}

print(connection_details)
