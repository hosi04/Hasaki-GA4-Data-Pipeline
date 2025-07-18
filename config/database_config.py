from dataclasses import dataclass
from typing import Dict
from dotenv import load_dotenv
import os

# SupperClass
class DatabaseConfig():
    def validate(self) -> None:
        for key, value in self.__dict__.items():
            if value is None:
                raise ValueError(f"-------------------Missing Value of key: {key}-------------------")

# Inheritance from Class DatabaseConfig
# SubClass
@dataclass
class ClickHouseConfig(DatabaseConfig):
    host: str
    port: int
    user: str
    password: str
    database: str
    table: str = "ga_raw_events"


def get_database_config() -> Dict[str, DatabaseConfig]:
    load_dotenv()
    config = {
        "clickhouse": ClickHouseConfig(
            host = os.getenv("CLICKHOUSE_HOST"),
            port = int(os.getenv("CLICKHOUSE_PORT")),
            user = os.getenv("CLICKHOUSE_USER"),
            password = os.getenv("CLICKHOUSE_PASSWORD"),
            database = os.getenv("CLICKHOUSE_DATABASE")
        )
    }
    for key,value in config.items():
        value.validate()
    return config