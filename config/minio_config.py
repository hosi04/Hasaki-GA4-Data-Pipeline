import os
from dataclasses import dataclass
from typing import Dict

from dotenv import load_dotenv


# SupperClass
class MinioConfig():
    def validate(self) -> None:
        for key, value in self.__dict__.items():
            if value is None:
                raise ValueError(f"-------------------Missing Value of key: {key}-------------------")

# Inheritance from Class DatabaseConfig
# SubClass
@dataclass
class MinioConfig(MinioConfig):
    access_key: str
    secret_key: str
    endpoint: str
    bucket_name: str


def get_minio_config() -> Dict[str, MinioConfig]:
    load_dotenv("/opt/airflow/.env")
    config = {
        "minio": MinioConfig(
            access_key = os.getenv("ACCESS_KEY"),
            secret_key = os.getenv("SECRET_KEY"),
            endpoint = os.getenv("ENDPOINT"),
            bucket_name = os.getenv("BUCKET_NAME")
        )
    }
    for key,value in config.items():
        value.validate()
    return config