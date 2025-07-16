from config.database_config import get_database_config
from database.clickhouse_connection import ClickHouseConnect
from database.schema_manager import create_clickhouse_schema


def main(config):
    with ClickHouseConnect(config["clickhouse"].host, config["clickhouse"].port, config["clickhouse"].user, config["clickhouse"].password) as clickhouse_client:
        client = clickhouse_client.client
        create_clickhouse_schema(client)

if __name__ == "__main__":
    config = get_database_config()
    main(config)