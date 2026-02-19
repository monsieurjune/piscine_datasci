import psycopg2 as ps
from getpass import getpass
from os import path
from typing import Dict

TABLE_NAME = "customers"

PL_SCRIPT = f"""
DROP FUNCTION IF EXISTS remove_backend_mess();

ALTER TABLE {TABLE_NAME} ADD COLUMN id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY;

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
		FROM {TABLE_NAME}
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

	DELETE FROM {TABLE_NAME} WHERE id = ANY(duplicated_id_set);
END;
$$;

SELECT remove_backend_mess();

ALTER TABLE {TABLE_NAME} DROP COLUMN id;
"""

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

        # Run PL Script
        cur.execute(PL_SCRIPT)

        # Commit
        conn.commit()

        # Close
        cur.close()
        conn.close()

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
