import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """Drop existing tables based on the queries in drop_table_queries."""
    print("Starting to drop tables...")
    for i, query in enumerate(drop_table_queries, start=1):
        try:
            table_name = query.split()[4]  # Extract table name from SQL query
            print(f"Dropping table: {table_name} ({i}/{len(drop_table_queries)})...")
            cur.execute(query)
            conn.commit()
            print(f"Table {table_name} dropped successfully.")
        except Exception as e:
            print(f"Error dropping table {table_name}: {e}")
    print("All drop table operations completed.")


def create_tables(cur, conn):
    """Create tables based on the queries in create_table_queries."""
    print("Starting to create tables...")
    for i, query in enumerate(create_table_queries, start=1):
        try:
            table_name = query.split()[5]  # Extract table name from SQL query
            print(f"Creating table: {table_name} ({i}/{len(create_table_queries)})...")
            cur.execute(query)
            conn.commit()
            print(f"Table {table_name} created successfully.")
        except Exception as e:
            print(f"Error creating table {table_name}: {e}")
    print("All create table operations completed.")


def main():
    """Main function to connect to the database, drop tables, and create tables."""
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    try:
        print("Connecting to the database...")
        conn = psycopg2.connect(
            "host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values())
        )
        print("Connection established successfully.")
        cur = conn.cursor()

        # Drop existing tables and create new ones
        drop_tables(cur, conn)
        create_tables(cur, conn)

    except Exception as e:
        print(f"Error during database operations: {e}")
    finally:
        if conn:
            conn.close()
            print("Database connection closed.")


if __name__ == "__main__":
    main()