import psycopg2 as ps
from os import listdir
from getpass import getpass

def main() -> None:
    # Take Input
    csv_customer_path = input("CUSTOMER_DIR_PATH: ")
    hostname = input("HOST: ")
    db_name = input("POSTGRES_DB: ")
    user_name = input("POSTGRES_USER: ")
    passwd = getpass(prompt="POSTGRES_PASSWORD: ")

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
                table_name = file.replace(".csv", "") # remove .csv

                # Read CSV File
                with open(f"{csv_customer_path}/{file}", mode='r', newline='', encoding='utf-8') as file:
                    # Copy CSV content to psql via STDIN
                    cur.copy_expert(
                        f"""
                        COPY {table_name} 
                        (event_time, event_type, product_id, price, user_id, user_session)
                        FROM STDIN WITH (FORMAT csv, HEADER true)
                        """,
                        file=file
                    )

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
