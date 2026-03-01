import psycopg2 as ps
from os import path
from getpass import getpass
from typing import Dict, Callable

ITEMS_TABLE_NAME = "items"
CUSTOMERS_TABLE_NAME = "customers"
BATCH_SIZE = 100000

pl_remove_dup_items = lambda items_table, items_tmp_table: \
f"""
DROP FUNCTION IF EXISTS remove_dup_items;
DROP TABLE IF EXISTS {items_tmp_table};

CREATE TABLE {items_tmp_table} (
	product_id INTEGER,
	category_id BIGINT,
    category_code TEXT,
    brand TEXT
);

ALTER TABLE {items_table} ADD COLUMN id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY;

CREATE INDEX IF NOT EXISTS idx_{items_table} ON {items_tmp_table}(product_id);

CREATE FUNCTION remove_dup_items()
RETURNS VOID
LANGUAGE plpgsql
AS $$
DECLARE
	items_i RECORD;
	legit_score INTEGER := 0;
	highest_legit_score INTEGER := 0;
	legit_items_id_set BIGINT[];
	curr_items_id BIGINT := 0;
	curr_items_product_id INTEGER := 0;
BEGIN
	FOR items_i IN 
		SELECT *, LEAD(id) OVER items_w AS next_id
		FROM {items_table}
		WINDOW items_w AS (
			PARTITION BY product_id
			ORDER BY id
		)
	LOOP
		legit_score := 0;

		-- Check if items_i move into new window
		IF curr_items_product_id != items_i.product_id
		THEN
			curr_items_product_id := items_i.product_id;
			highest_legit_score := 0;
			curr_items_id := items_i.id;
		ELSE
		END IF;

		IF highest_legit_score < 3
		THEN
			-- Check legitimacy of category_id
			IF items_i.category_id IS NOT NULL
			THEN
				legit_score := legit_score + 1;
			ELSE
			END IF;
	
			-- Check legitimacy of category_code
			IF items_i.category_code IS NOT NULL
			THEN
				legit_score := legit_score + 1;
			ELSE
			END IF;
	
			-- Check legitimacy of brand
			IF items_i.brand IS NOT NULL
			THEN
				legit_score := legit_score + 1;
			ELSE
			END IF;
	
			-- Check if legit_score > highest_legit_score
			IF legit_score > highest_legit_score
			THEN
				highest_legit_score := legit_score;
				curr_items_id := items_i.id;
			ELSE
			END IF;
		ELSE
		END IF;

		-- Save legit id into vector
		IF highest_legit_score = 3
		THEN
			legit_items_id_set := array_append(legit_items_id_set, curr_items_id);
			highest_legit_score := 69;
		ELSIF highest_legit_score < 3 AND items_i.next_id IS NULL
		THEN
			legit_items_id_set := array_append(legit_items_id_set, curr_items_id);
		ELSE
		END IF;
	END LOOP;

	INSERT INTO {items_tmp_table} (
		product_id,
		category_id,
        category_code,
        brand
	)
	SELECT
		product_id,
		category_id,
        category_code,
        brand
	FROM {items_table} WHERE id = ANY(legit_items_id_set);
END;
$$;

SELECT remove_dup_items();

DROP INDEX idx_{items_table};

ALTER TABLE {items_table} DROP COLUMN id;
"""

pl_left_join = lambda customers_table, customers_tmp_table, items_tmp_table, batch_n: \
f"""
DROP FUNCTION IF EXISTS paging_join;
DROP TABLE IF EXISTS {customers_tmp_table};

CREATE TABLE {customers_tmp_table} (
	event_time TIMESTAMPTZ,
    event_type TEXT,
    product_id INTEGER,
    price NUMERIC(8, 2), 
    user_id BIGINT,
    user_session UUID,
    category_id BIGINT,
    category_code TEXT,
    brand TEXT
);

ALTER TABLE {customers_table} ADD COLUMN id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY;

CREATE INDEX IF NOT EXISTS idx_{customers_table} ON {customers_table}(id, product_id);
CREATE INDEX IF NOT EXISTS idx_{items_tmp_table} ON {items_tmp_table}(product_id);

CREATE FUNCTION paging_join()
RETURNS VOID
LANGUAGE plpgsql
AS $$
DECLARE
	max_id INT;
	start_id INT := 1;
	paging_size INT := {batch_n};
BEGIN
	SELECT MAX(id) INTO max_id FROM {customers_table};

	WHILE start_id <= max_id
	LOOP
		INSERT INTO {customers_tmp_table} (
			event_time,
            event_type,
            product_id,
            price, 
            user_id,
            user_session,
            category_id,
            category_code,
            brand
		)
		SELECT
			{customers_table}.event_time,
            {customers_table}.event_type,
            {customers_table}.product_id,
            {customers_table}.price, 
            {customers_table}.user_id,
            {customers_table}.user_session,
            {items_tmp_table}.category_id,
            {items_tmp_table}.category_code,
            {items_tmp_table}.brand
		FROM {customers_table} LEFT JOIN {items_tmp_table}
		ON {customers_table}.product_id = {items_tmp_table}.product_id
		WHERE {customers_table}.id >= start_id AND {customers_table}.id < start_id + paging_size;

		start_id := start_id + paging_size;
	END LOOP;
END;
$$;

SELECT paging_join();

DROP INDEX idx_{customers_table};
DROP INDEX idx_{items_tmp_table};

ALTER TABLE {customers_table} DROP COLUMN id;
"""

pl_clean_up = lambda customers_table, customers_tmp_table, items_tmp_table: \
f"""
DROP TABLE {customers_table};
DROP TABLE {items_tmp_table};

ALTER TABLE {customers_tmp_table} RENAME TO {customers_table};

DROP FUNCTION remove_dup_items;
DROP FUNCTION paging_join;
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

        # Variable
        items_tmp_table = f"{ITEMS_TABLE_NAME}_tmp"
        customers_tmp_table = f"{CUSTOMERS_TABLE_NAME}_tmp"

        # Creating clean version of items table
        cur.execute(
            pl_remove_dup_items(
                ITEMS_TABLE_NAME, 
                items_tmp_table
            )
        )

        # Left Join customers to items_2nd table into customers_2nd table
        cur.execute(
            pl_left_join(
                CUSTOMERS_TABLE_NAME, 
                customers_tmp_table, 
                items_tmp_table, 
                BATCH_SIZE
            )
        )

        # Cleaning up the messes
        cur.execute(
            pl_clean_up(
                CUSTOMERS_TABLE_NAME,
                customers_tmp_table,
                items_tmp_table
            )
        )

        # Commit
        conn.commit()

        # Close
        cur.close()
        conn.close()

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
