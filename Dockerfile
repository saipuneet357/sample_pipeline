FROM puckel/docker-airflow

# Install python libraries
RUN pip3 install boto3
RUN pip3 install sqlalchemy
RUN pip3 install pandas
