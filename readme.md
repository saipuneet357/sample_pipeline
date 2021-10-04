Change data source and data store details in config.py

To test a run you can change the config file and run tasks.py

To test a scheduled run you can build a docker image and run it using the follow steps:
Run these commands:
----------------------------------------------------------------------------------
1. docker build -t test_dag .
2. docker run -d -p 8880:8080 -v /path/to/dag/file/:/usr/local/airflow/dags testdag

Change the path/to/dag/file according to the path where you place this code
----------------------------------------------------------------------------------
3. open localhost:8880 and enable the airflow dag sample_pipeline from the ui. The dag will be enabled
and will work as scheduled


Assumptions Made:

1) Since the loan application data doesn't have any primary key. I am pushing the file into a staging table
and then inserting new data into the main table while deactivating old data.

2) Using s3 as the source data and redshift as the data store
3) The dag is scheduled daily

A brief statistical presentation of the data is done in the file Understanding Loan Application Data.pptx
