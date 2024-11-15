from urllib.parse import urlparse

from clickhouse_driver import Client
from clickhouse_driver.errors import NetworkError, ServerException


class ClickhouseService:

    @staticmethod
    def check_connection(connection_string):
        try:
            # Разбираем строку подключения
            parsed_url = urlparse(connection_string)

            if parsed_url.scheme != "clickhouse":
                print("Invalid connection string scheme. Expected 'clickhouse'")
                return False

            # Извлекаем параметры из строки подключения
            user = parsed_url.username or 'default'
            password = parsed_url.password or ''
            host = parsed_url.hostname or 'localhost'
            port = parsed_url.port or 9000
            database = parsed_url.path.lstrip('/') or 'default'
            print(user, password, host, port, database)

            # Инициализируем клиента ClickHouse
            client = Client(
                host=host,
                port=port,
                user=user,
                password=password,
                database=database
            )

            # Пробуем выполнить простой запрос
            client.execute("SELECT 1")
            return True

        except (NetworkError, ServerException) as e:
            print(f"Connection failed: {e}")
            return False
        except Exception as e:
            print(f"Unexpected error: {e}")
            return False
    
    def dump_database(self, connection_string):
        print("connection_string", connection_string)