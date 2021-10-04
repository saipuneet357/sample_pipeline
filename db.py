import sqlalchemy as db


def engine(host, user, password, database, port=3306):
    # specify connection string
    connection_str = f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}?client_encoding=utf8'
    # connect to database
    return db.create_engine(connection_str, pool_size=10)


if __name__ == '__main__':
    host = 'localhost'
    user = 'spuneet'
    password = 'newpassword'
    database = 'samplepipeline'
    port = 3306
    e = engine(host, user, password, database, port)
