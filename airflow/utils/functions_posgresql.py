import os
import psycopg2
from sqlalchemy import create_engine, text
import pandas as pd
import tiktoken
from fastapi.logger import logger
from functions_general import *
from fastapi.encoders import jsonable_encoder
from copy import copy, deepcopy

host_server = os.environ.get('host_server', 'hiai.postgres.database.azure.com')
db_server_port = os.environ.get('db_server_port', '5432')
database_name = os.environ.get('database_name', 'fastapi')
db_username = os.environ.get('db_username', 'Mosmos81')
db_password = os.environ.get('db_password', 'Welcome2022!')
ssl_mode = os.environ.get('ssl_mode')

connect_alchemy = "postgresql+psycopg2://%s:%s@%s/%s" % (
    db_username,
    db_password,
    host_server,
    database_name
)

engine = create_engine(connect_alchemy)


def readFile(filename):
    fd = open(filename, 'r')
    sqlFile = fd.read()
    fd.close()
    return sqlFile


def get_data_from_db(query):
    try:
        conn = psycopg2.connect(os.environ.get('databaseurl'))
        cursor = conn.cursor()
        cursor.execute(query)
        records = cursor.fetchall()
        return records
    except (Exception, psycopg2.Error) as error:
        print("Error while fetching data from PostgreSQL", error)
    finally:
        if conn:
            cursor.close()
            conn.close()


def get_one_from_db(query):
    try:
        conn = psycopg2.connect(os.environ.get('databaseurl'))
        cursor = conn.cursor()
        cursor.execute(query)
        records = cursor.fetchone()
        return records[0]
    except (Exception, psycopg2.Error) as error:
        print("Error while fetching data from PostgreSQL", error)
    finally:
        if conn:
            cursor.close()
            conn.close()


def get_data_as_pd(query):
    host_server = os.environ.get('host_server')
    database_name = os.environ.get('database_name')
    db_username = os.environ.get('db_username')
    db_password = os.environ.get('db_password')

    connect_alchemy = "postgresql+psycopg2://%s:%s@%s/%s" % (
        db_username,
        db_password,
        host_server,
        database_name
    )

    engine = create_engine(connect_alchemy)

    with engine.connect() as conn:
        return pd.read_sql(text(query), conn)


def insert_pd_into_postgresql(df, tableName):
    host_server = os.environ.get('host_server')
    database_name = os.environ.get('database_name')
    db_username = os.environ.get('db_username')
    db_password = os.environ.get('db_password')

    connect_alchemy = "postgresql+psycopg2://%s:%s@%s/%s" % (
        db_username,
        db_password,
        host_server,
        database_name
    )

    engine = create_engine(connect_alchemy)

    df.to_sql(tableName, con=engine, index=False, if_exists='append', chunksize=1000)


def update_posgresql_db(query):
    try:
        conn = psycopg2.connect(os.environ.get('databaseurl'))
        conn.autocommit = True
        cursor = conn.cursor()
        cursor.execute(query)
    except (Exception, psycopg2.Error) as error:
        print("Error while fetching data from PostgreSQL", error)
    finally:
        if conn:
            cursor.close()
            conn.close()
