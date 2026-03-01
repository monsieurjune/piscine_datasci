import psycopg2 as ps
from os import path
from getpass import getpass
from typing import Dict, Callable

MAIN_TABLE_NAME = "customers"

sql_listing_query = lambda: \
f"""
SELECT table_name FROM information_schema.tables 
WHERE table_schema IN ('public') AND table_name LIKE 'data\\_202_\\____' ESCAPE '\\';
"""

sql_creating_query = lambda main_table_name: \
f"""
CREATE TABLE IF NOT EXISTS {main_table_name} (
    event_time TIMESTAMPTZ, 
    event_type TEXT, 
    product_id INTEGER, 
    price NUMERIC(8, 2), 
    user_id BIGINT, 
    user_session UUID
);
"""

sql_append_query = lambda main_table_name, sub_table_name: \
f"""
INSERT INTO {main_table_name} SELECT * FROM {sub_table_name};
"""

def get_env() -> Dict[str, str]:
    dict: Dict[str, str] = {}

    try:
        if not path.isfile('.env'):
            return dict

        with open('.env', mode='r', newline='', encoding='utf-8') as file:
            raw_lines = file.readlines()

            for raw_line in raw_lines:
                ret = raw_line.replace('\n', '').split('=', 2)
                if ret.__len__() == 2:
                    dict[ret[0]] = ret[1]

        return dict
    except OSError as _:
        return {}

def fill_env(env: Dict[str, str], key: str, fn: Callable[..., str]) -> None:
    if env.get(key) is None:
        env[key] = fn(f"{key}: ")

def init_env() -> Dict[str, str]:
    env = get_env()

    fill_env(env, "POSTGRES_HOSTNAME", input)
    fill_env(env, "POSTGRES_DB", input)
    fill_env(env, "POSTGRES_USER", input)
    fill_env(env, "POSTGRES_PASSWORD", getpass)

    return env

def main() -> None:
    env = init_env()

    # Setup var
    hostname: str = env["POSTGRES_HOSTNAME"]
    db_name: str = env["POSTGRES_DB"]
    user_name: str = env["POSTGRES_USER"]
    passwd: str = env["POSTGRES_PASSWORD"]

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
        cur.execute(sql_listing_query())
        customer_tables = cur.fetchall()

        # Run Creating Table Query
        cur.execute(sql_creating_query(MAIN_TABLE_NAME))

        # Run Append Query
        # Note that this script doesn't prevent redundant copy
        for table, in customer_tables:
            cur.execute(sql_append_query(MAIN_TABLE_NAME, table))

        # Commit
        conn.commit()

        # Close
        cur.close()
        conn.close()

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
