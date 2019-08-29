import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    print("DROPing tables: {} operation(s) to execute".format(
        len(drop_table_queries)
    ))
    for query in drop_table_queries:
        print('DROPing table...', end='')
        cur.execute(query)
        conn.commit()
        print('OK!')


def create_tables(cur, conn):
    print("CREATing tables: {} operation(s) to execute".format(
        len(create_table_queries)
    ))
    for query in create_table_queries:
        print('CREATing table...', end='')
        cur.execute(query)
        conn.commit()
        print('OK!')


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect(
            "host={} dbname={} user={} password={} port={}".format(
                *config['CLUSTER'].values()
            )
    )
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
