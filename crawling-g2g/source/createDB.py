# /e/Fear/Univ/Big_data/Training/python/python-crawling/venv/Scripts/python
# -*- encoding: utf-8 -*-

from psycopg2 import connect, extensions

def main():
    # step 01 연결
    conn = connecting()
    print(conn)

    createDB(conn)

def connecting():
    # DB Connect
    conn = connect(
        host = "localhost", # SQL 서버 주소
        dbname = "postgres",
        user = "postgres",
        password = "pwd",
        port = "5432"
    )

    # print(conn)
    return conn

def createDB(conn):
    print("createDB function.. in... ")
    DB_NAME = "DB_G2G"

    # AutoCommit
    autocommit = extensions.ISOLATION_LEVEL_AUTOCOMMIT
    print("ISOLATION_LEVEL_AUTOCOMMIT:", extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    conn.set_isolation_level(autocommit)

    QUERY = '''CREATE DATABASE {DBNAME}'''.format(DBNAME = DB_NAME);

    cursor = conn.cursor()
    cursor.execute(QUERY)
    print("Database created...")

    cursor.close()

    conn.close()

if __name__ == "__main__":
    main()