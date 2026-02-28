import psycopg2 as ps
from os import listdir, path
from getpass import getpass
from typing import Dict, Callable

sql_query = lambda table_name: \
f"""
CREATE TABLE {table_name} (
    event_time TIMESTAMPTZ, 
    event_type TEXT, 
    product_id INTEGER, 
    price NUMERIC(8, 2), 
    user_id BIGINT, 
    user_session UUID
);
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

    fill_env(env, "CUSTOMER_DIR_PATH", input)
    fill_env(env, "HOST", input)
    fill_env(env, "POSTGRES_DB", input)
    fill_env(env, "POSTGRES_USER", input)
    fill_env(env, "POSTGRES_PASSWORD", getpass)

    return env

def main() -> None:
    env = init_env()

    # Setup var
    csv_customer_path: str = env["CUSTOMER_DIR_PATH"]
    hostname: str = env["HOST"]
    db_name: str = env["POSTGRES_DB"]
    user_name: str = env["POSTGRES_USER"]
    passwd: str = env["POSTGRES_PASSWORD"]

    try:
        # List thing in dir
        enties = listdir(csv_customer_path)
        csv_files = [f for f in enties if ".csv" in f]
        if (csv_files.__len__() < 1):
            return

        # Connect to server
        conn = ps.connect(
            dbname=db_name,
            user=user_name,
            password=passwd,
            host=hostname,
            port="5432"
        )
        cur = conn.cursor()

        # Run Query
        for file in csv_files:
            try:
                table_name = file.replace(".csv", "")
                cur.execute(sql_query(table_name))
            except Exception as e:
                print(f"Cursur Error: {e}")

        # Commit
        conn.commit()

        # Close
        cur.close()
        conn.close()

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
