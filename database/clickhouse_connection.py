import clickhouse_connect

class ClickHouseConnect:
    def __init__(self, host, port, user, password):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.client = None

    def connect(self):
        try:
            self.client = clickhouse_connect.get_client(
                host=self.host,
                port=self.port,
                username=self.user,
                password=self.password
            )
            print("---------------------Connected to ClickHouse database---------------------")
            return self.client
        except Exception as error:
            raise Exception(f"---------------------Failed to connect to ClickHouse: {error}---------------------")

    def close(self):
        self.client = None
        print("---------------------ClickHouse client has been closed---------------------")

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
