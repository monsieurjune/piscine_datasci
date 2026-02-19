import psycopg2 as ps
from getpass import getpass
from os import path
from typing import Dict

def get_env() -> Dict[str, str]:
    dict: Dict[str, str] = {}

    with open('.env', mode='r', newline='', encoding='utf-8') as file:
        raw_lines = file.readlines()

        for raw_line in raw_lines:
            ret = raw_line.replace('\n', '').split('=', 2)
            if ret.__len__() == 2:
                dict[ret[0]] = ret[1]

    return dict

def main() -> None:
    # Check .env file
    is_using_env = False
    try:
        is_using_env = path.isfile('.env')
    except Exception as _:
        pass

    # Setup var
    hostname: str | None = None
    db_name: str | None = None
    user_name: str | None = None
    passwd: str | None = None

    if is_using_env:
        # Read .env
        env = get_env()
        hostname = env.get('POSTGRES_HOSTNAME')
        db_name = env.get('POSTGRES_DB')
        user_name = env.get('POSTGRES_USER')
        passwd = env.get('POSTGRES_PASSWORD')

    # Take Input, in case that certain var is None
    if hostname is None:
        hostname = input("POSTGRES_HOSTNAME: ")
    if db_name is None:
        db_name = input("POSTGRES_DB: ")
    if user_name is None:
        user_name = input("POSTGRES_USER: ")
    if passwd is None:
        passwd = getpass(prompt="POSTGRES_PASSWORD: ")

    try:
        # Connect to server
        conn = ps.connect(
            dbname=db_name,
            user=user_name,
            password=passwd,
            host=hostname,
            port="5432"
        )
        cur = conn.cursor()

        # Run Listing Query
        query = f"SELECT table_name FROM information_schema.tables WHERE table_schema IN ('public') AND table_name LIKE 'data\\_202_\\____' ESCAPE '\\';"
        cur.execute(query)
        customer_tables = cur.fetchall()

        # Run Creating Table Query
        main_table_name = "customers"
        query = f"CREATE TABLE IF NOT EXISTS {main_table_name} ( event_time TIMESTAMPTZ, event_type TEXT, product_id INTEGER, price NUMERIC(8, 2), user_id BIGINT, user_session UUID );"
        cur.execute(query)

        # Run Append Query
        # Note that this script doesn't prevent redundant copy
        for table, in customer_tables:
            query = f"INSERT INTO {main_table_name} SELECT * FROM {table}"
            cur.execute(query)

        # Commit
        conn.commit()

        # Close
        cur.close()
        conn.close()

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
