import sqlalchemy as db


def engine(host, user, password, database, port=3306):
    # specify connection string
    connection_str = f'mysql+pymysql://{user}:{password}@{host}:{port}/{database}'
    # connect to database
    return db.create_engine(connection_str)


if __name__ == '__main__':
    e = engine('localhost', 'spuneet', 'newpassword', 'samplepipeline', 3307)
