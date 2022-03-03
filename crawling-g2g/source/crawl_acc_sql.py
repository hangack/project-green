import pandas_gbq
import psycopg2
from psycopg2 import connect, extensions
import psycopg2.extras as extras
from google.oauth2 import service_account


def connecting():
    # DB Connect
    conn = connect(
        host = "localhost",
        dbname = "db_g2g",
        user = "postgres",
        password = "pwd",
        port = "5432"
    )

    return conn

def dataInsertPsycopg2(conn, data):
    # Single Insert
    TABLE_NAME = "poe"

    # AutoCommit
    autocommit = extensions.ISOLATION_LEVEL_AUTOCOMMIT
    print("ISOLATION_LEVEL_AUTOCOMMIT:", extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    conn.set_isolation_level(autocommit)

    tuples = [tuple(x) for x in data.to_numpy()]

    cols = ','.join(list(data.columns))
    print(cols) # STATION,GETON_PPL,GETOFF_PPL


    # SQL query to execute
    query = "INSERT INTO %s(%s) VALUES %%s" % (TABLE_NAME, cols)
    print(query)


    cursor = conn.cursor()
    # https://www.psycopg.org/docs/extras.html
    try:
        extras.execute_values(cursor, query, argslist = tuples)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    print("the dataframe is inserted")
    cursor.close()
    conn.close()


def accGBQ(df):
    credentials = service_account.Credentials.from_service_account_info(
        {
            "type": "",
            "project_id": "",
            "private_key_id": "",
            "private_key": "",
            "client_email": "",
            "client_id": "",
            "auth_uri": "",
            "token_uri": "",
            "auth_provider_x509_cert_url": "",
            "client_x509_cert_url": ""
        }
    )

    pandas_gbq.context.credentials = credentials
    pandas_gbq.context.project = "g2g-crawling"

    table_name = "DB_G2G.poe"
    project_id = "g2g-crawling"

    pandas_gbq.to_gbq(df, table_name, project_id=project_id, if_exists="append")

    print('to_GBQ: migration complete')