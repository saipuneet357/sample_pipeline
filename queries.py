
class SQLQueries:
    def __init__(self):
        self.load_data_query = '''
        LOAD DATA LOCAL INFILE 'data.csv'
        INTO TABLE loan_application_details
        FIELDS TERMINATED BY ','
        ENCLOSED BY '"'
        LINES TERMINATED BY '\n'
        IGNORE 1 ROWS;
        '''

        self.create_table_query = '''
        create table if not exists loan_application_details (
        seriousdlqin2yrs varchar(16),
        resolving_utilization_of_unsecured_lines varchar(16),
        age int,
        number_of_time3059_days_past_due_not_worse int,
        debt_ratio varchar(16),
        monthly_income numeric(38, 8),
        number_of_open_credit_lines_and_loans int,
        number_of_times_90_days_late int,
        number_real_estate_loans_or_lines int,
        number_of_time6089_days_past_due_not_worse int,
        number_of_dependents int
        );
        '''
