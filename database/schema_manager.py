from pathlib import Path
from clickhouse_connect.driver.exceptions import ClickHouseError

def create_clickhouse_schema(client):
    SQL_FILE_PATH = Path("../sql/clickhouse_schema.sql")
    DATABASE_NAME = "hskcdp_ga"

    try:
        client.command(f"DROP DATABASE IF EXISTS {DATABASE_NAME}")
        client.command(f"CREATE DATABASE IF NOT EXISTS {DATABASE_NAME}")
        client.command(f"USE {DATABASE_NAME}")

        # Read and execute schema.sql
        with open(SQL_FILE_PATH, 'r') as sql_file:
            sql_script = sql_file.read()
            commands = [cmd.strip() for cmd in sql_script.split(";") if cmd.strip()]
            for cmd in commands:
                client.command(cmd)

        print("---------------------ClickHouse schema created successfully---------------------")

    except ClickHouseError as e:
        raise Exception(f"Failed to create ClickHouse schema: {e}") from e


