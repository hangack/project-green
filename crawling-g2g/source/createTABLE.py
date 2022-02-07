# /e/Fear/Univ/Big_data/Training/python/python-crawling/venv/Scripts/python
# -*- encoding: utf-8 -*-

from psycopg2 import connect, extensions

def main():
    # step 01 연결
    conn = connecting()
    print(conn)

    createTable(conn)

def connecting():
    # DB Connect
    conn = connect(
        host = "localhost",
        dbname = "db_g2g",
        user = "postgres",
        password = "pwd",
        port = "5432"
    )

    # print(conn)
    return conn

def createTable(conn):
    print("createTable function.. in... ")
    TABLE_NAME = "poe"

    # AutoCommit
    autocommit = extensions.ISOLATION_LEVEL_AUTOCOMMIT
    print("ISOLATION_LEVEL_AUTOCOMMIT:", extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    conn.set_isolation_level(autocommit)

    # 기존 테이블 삭제
    cursor = conn.cursor()
    cursor.execute("DROP TABLE IF EXISTS TEMP")
    print("Table DELETED....!")

    # 테이블 생성 작성
    QUERY = '''
        CREATE TABLE {NAME}(
            date CHAR(16),
            seller CHAR(32),
            server CHAR(50),
            currency CHAR(50),
            price CHAR(32),
            stock CHAR(32)
        )
    '''.format(NAME = TABLE_NAME);

    cursor.execute(QUERY)
    print("Table created...")

    cursor.close()

    conn.close()

if __name__ == "__main__":
    main()