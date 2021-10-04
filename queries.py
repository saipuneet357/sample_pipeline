
class SQLQueries:
    def __init__(self):
        self.load_data_query = '''
        COPY stg_loan_application_details
        FROM 's3://{bucket_name}/{file_name}'
        with credentials
        'aws_access_key_id={access_key};aws_secret_access_key={secret_key}'
        CSV IGNOREHEADER 1
        region as '{region}';
        '''

        self.create_staging_table_query = '''
        drop table if exists stg_loan_application_details;

        create table stg_loan_application_details (
        seriousdlqin2yrs int,
        resolving_utilization_of_unsecured_lines numeric(38, 8),
        age numeric(38, 8),
        number_of_time3059_days_past_due_not_worse numeric(38, 8),
        debt_ratio numeric(38, 8),
        monthly_income numeric(38, 8),
        number_of_open_credit_lines_and_loans numeric(38, 8),
        number_of_times_90_days_late numeric(38, 8),
        number_real_estate_loans_or_lines numeric(38, 8),
        number_of_time6089_days_past_due_not_worse numeric(38, 8),
        number_of_dependents numeric(38, 8)
        );
        '''

        self.create_table_query = '''
        create table if not exists loan_application_details (
        seriousdlqin2yrs int,
        resolving_utilization_of_unsecured_lines numeric(38, 8),
        age numeric(38, 8),
        number_of_time3059_days_past_due_not_worse numeric(38, 8),
        debt_ratio numeric(38, 8),
        monthly_income numeric(38, 8),
        number_of_open_credit_lines_and_loans numeric(38, 8),
        number_of_times_90_days_late numeric(38, 8),
        number_real_estate_loans_or_lines numeric(38, 8),
        number_of_time6089_days_past_due_not_worse numeric(38, 8),
        number_of_dependents numeric(38, 8),
        deactivated_date date
        );
        '''

        self.deactivate_existing_records = '''
        update loan_application_details
        set deactivated_date = current_date
        where deactivated_date is null;
        '''

        self.insert_into_main = '''
        insert into loan_application_details
        select *,null from stg_loan_application_details;
        '''
