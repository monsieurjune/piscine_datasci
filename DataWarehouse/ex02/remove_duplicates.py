import psycopg2 as ps
from os import path
from getpass import getpass
from typing import Dict, Callable

MAIN_TABLE_NAME = "customers"

pl_script = lambda table_name: \
f"""
DROP FUNCTION IF EXISTS remove_backend_mess();

ALTER TABLE {table_name} ADD COLUMN id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY;

CREATE FUNCTION remove_backend_mess()
RETURNS VOID
LANGUAGE plpgsql
AS $$
DECLARE
	tran_i RECORD;
	duplicated_id_set BIGINT[];
	curr_real_time TIMESTAMPTZ := TIMESTAMPTZ '1970-01-01 00:00:00+00';
BEGIN
	FOR tran_i IN
		SELECT *, LAG(event_time) OVER u_trans_w AS prev_time
		FROM {table_name}
		WINDOW u_trans_w AS (
			PARTITION BY user_id, user_session, event_type, product_id
			ORDER BY event_time
		)
	LOOP
		IF tran_i.prev_time IS NULL
		THEN
			curr_real_time := tran_i.event_time;
		ELSIF tran_i.event_time > curr_real_time + INTERVAL '1 second'
		THEN
			curr_real_time := tran_i.event_time;
		ELSE
			duplicated_id_set := array_append(duplicated_id_set, tran_i.id);
		END IF;
	END LOOP;

	DELETE FROM {table_name} WHERE id = ANY(duplicated_id_set);
END;
$$;

SELECT remove_backend_mess();

ALTER TABLE {table_name} DROP COLUMN id;
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

        # Run PL Script
        cur.execute(pl_script(MAIN_TABLE_NAME))

        # Commit
        conn.commit()

        # Close
        cur.close()
        conn.close()

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
