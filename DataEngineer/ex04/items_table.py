import psycopg2 as ps
from os import listdir, path
from getpass import getpass
from typing import Dict, Callable

sql_create_query = lambda table_name: \
f"""
CREATE TABLE {table_name} (
    product_id INTEGER, 
    category_id BIGINT, 
    category_code TEXT, 
    brand TEXT
);
"""

sql_copy_query = lambda table_name: \
f"""
COPY {table_name} (
    product_id, 
    category_id, 
    category_code, 
    brand
)
FROM STDIN WITH (FORMAT csv, HEADER true);
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

    fill_env(env, "ITEMS_DIR_PATH", input)
    fill_env(env, "HOST", input)
    fill_env(env, "POSTGRES_DB", input)
    fill_env(env, "POSTGRES_USER", input)
    fill_env(env, "POSTGRES_PASSWORD", getpass)

    return env

def main() -> None:
    env = init_env()

    # Setup var
    csv_items_path: str = env["ITEMS_DIR_PATH"]
    hostname: str = env["HOST"]
    db_name: str = env["POSTGRES_DB"]
    user_name: str = env["POSTGRES_USER"]
    passwd: str = env["POSTGRES_PASSWORD"]

    try:
        # List thing in dir
        enties = listdir(csv_items_path)
        if ("item.csv" not in enties):
            print("There is no item.csv file in this dir")
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
        try:
            table_name = "items"

            # Create Table
            cur.execute(sql_create_query(table_name))
            conn.commit()

            # Read CSV File
            with open(f"{csv_items_path}/item.csv", mode='r', newline='', encoding='utf-8') as file:
                # Copy CSV content to psql via STDIN
                cur.copy_expert(sql_copy_query(table_name), file=file)

                # Commit per table
                conn.commit()
        except Exception as e:
            print(f"Cursur Error: {e}")

        # Close
        cur.close()
        conn.close()

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
