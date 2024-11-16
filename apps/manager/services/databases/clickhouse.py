import os
from urllib.parse import urlparse
import shutil
import subprocess
import tempfile
import zipfile

from clickhouse_driver import Client
from clickhouse_driver.errors import NetworkError, ServerException


class ClickhouseService:

    def parse_connection_string(self, connection_string):
        parsed_url = urlparse(connection_string)
        user = parsed_url.username or 'default'
        password = parsed_url.password or ''
        host = parsed_url.hostname or 'localhost'
        port = parsed_url.port or 9000
        database = parsed_url.path.lstrip('/') or 'default'
        return user, password, host, port, database

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

    def _create_config(self, connection_string):
        user, password, host, port, database = self.parse_connection_string(connection_string)
        # Динамически создаём временный конфиг
        config_content = f"""
clickhouse:
  host: {host}
  port: {port}
  username: {user}
  password: {password}
  database: {database}
  timeout: 10s

general:
  remote_storage: s3
  max_file_size: 10737418240

s3:
  access_key: aaa
  secret_key: aaa
  bucket: aaa
  endpoint: aaaa
  use_ssl: false
  path: "/"
"""
        try:
            with tempfile.NamedTemporaryFile(delete=False, suffix=".yml") as temp_config:
                config_file_path = temp_config.name
                temp_config.write(config_content.encode())
        except Exception as e:
            return None, f"Error cretate temp config: {e}"
        return config_file_path, None
    
    def dump_database(self, connection_string, operation_id):
        file_name = f"dump_{operation_id}"
        folder_prefix = "/var/lib/clickhouse/backup/"
        backup_path = os.path.join(folder_prefix, file_name)
        zip_file_path = f"/tmp/{file_name}.zip"

        config_file_path, error = self._create_config(connection_string)
        if error:
            return None, error

        try:
            command = f"clickhouse-backup create {file_name} --config {config_file_path}"
            subprocess.run(command, shell=True, check=True)
        except Exception as e:
            return None, f"Error executing command: {e}"
        finally:
            # Удаление временного файла конфигурации
            os.remove(config_file_path)
        try:
            # Упаковка папки в zip-архив
            with zipfile.ZipFile(zip_file_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
                for root, dirs, files in os.walk(backup_path):
                    for file in files:
                        file_path = os.path.join(root, file)
                        arcname = os.path.relpath(file_path, start=backup_path)
                        zipf.write(file_path, arcname)

            # Удаление папки с бэкапом после упаковки
            shutil.rmtree(backup_path)
        except Exception as e:
            return None, f"Error zipping or cleaning up: {e}"

        return zip_file_path, None
    
    def load_dump(self, connection_string, filepath):
        """Загрузка дампа в ClickHouse из zip-архива."""
        file_name = os.path.basename(filepath).replace(".zip", "")
        folder_prefix = "/var/lib/clickhouse/backup/"
        backup_path = os.path.join(folder_prefix, file_name)

        config_file_path, error = self._create_config(connection_string)
        if error:
            return False, error
        # Распаковываем архив в /var/lib/clickhouse/backup
        try:
            with zipfile.ZipFile(filepath, 'r') as zip_ref:
                zip_ref.extractall(backup_path)
        except Exception as e:
            os.remove(config_file_path)
            return False, f"Error extracting zip file: {e}"

        # Выполняем команду восстановления дампа
        try:
            command = f"clickhouse-backup restore {file_name} --config {config_file_path} --data"
            subprocess.run(command, shell=True, check=True)
        except subprocess.CalledProcessError as e:
            return False, f"Error restoring backup: {e}"
        finally:
            # Удаляем временный файл конфигурации
            os.remove(config_file_path)
            # Удаляем папку с бэкапом после восстановления
            shutil.rmtree(backup_path, ignore_errors=True)
        return True, None
