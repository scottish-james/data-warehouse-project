import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Iterate over list of load queries. This transfers the data from the S3
    buckets to the staging tables via COPY.
    """
    print("\nStarting to load staging tables...")
    for i, query in enumerate(copy_table_queries, 1):
        try:
            print(f"Executing COPY query {i}/{len(copy_table_queries)}...")
            cur.execute(query)
            conn.commit()
            print(f"COPY query {i}/{len(copy_table_queries)} executed successfully.")
        except Exception as e:
            print(f"Error while executing COPY query {i}: {e}")


def insert_tables(cur, conn):
    """
    Iterates over the list of insert queries. This transfers data from the
    staging tables to the final tables.
    """
    print("\nStarting to insert data into final tables...")
    for i, query in enumerate(insert_table_queries, 1):
        try:
            print(f"Executing INSERT query {i}/{len(insert_table_queries)}...")
            cur.execute(query)
            conn.commit()
            print(f"INSERT query {i}/{len(insert_table_queries)} executed successfully.")
        except Exception as e:
            print(f"Error while executing INSERT query {i}: {e}")


def main():
    """
    Read config, establish DWH connection and call load and insert functions.
    """
    print("Running...")

    # Load configuration
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    # Connect to Redshift
    try:
        conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
        cur = conn.cursor()
        print("Connection to Redshift established successfully.")

    except Exception as e:
        print(f"Error connecting to Redshift: {e}")
        return

    # Load data into staging tables
    load_staging_tables(cur, conn)

    # Insert data into final tables
    insert_tables(cur, conn)

    # Close the connection
    conn.close()
    print("ETL process complete.")


if __name__ == "__main__":
    main()

