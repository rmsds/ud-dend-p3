import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    ''' Load the staging tables using COPY commands.'''
    print("Loading staging tables: {} operation(s) to execute".format(
        len(copy_table_queries)
    ))
    for query in copy_table_queries:
        print('Loading staging table...', end='')
        cur.execute(query)
        conn.commit()
        print('OK!')


def insert_tables(cur, conn):
    ''' Transform and load data from the staging to the final tables. '''
    print("Inserting into tables: {} operation(s) to execute".format(
        len(insert_table_queries)
    ))
    for query in insert_table_queries:
        print('Inserting into table...', end='')
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

    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
