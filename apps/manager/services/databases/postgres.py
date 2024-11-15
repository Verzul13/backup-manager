import subprocess

import psycopg2


class PostgresqlService:

    @staticmethod
    def check_connection(connection_string):
        result = True
        try:
            # Устанавливаем соединение с базой данных
            conn = psycopg2.connect(connection_string)
            cursor = conn.cursor()
            
            # Выполняем запрос для получения версии PostgreSQL
            cursor.execute("SELECT version();")
            version = cursor.fetchone()[0]
            
            if version:
                version = version.split(" ")[1]
            
        except Exception as e:
            result = False
        finally:
            # Закрываем соединение
            if cursor:
                cursor.close()
            if conn:
                conn.close()
        return result

    def dump_database(self, connection_string, operation_id):
        output_file = f"/tmp/dump_{operation_id}.sql"
        try:
            # Формируем команду для pg_dump
            pg_dump_path = "/usr/lib/postgresql/15/bin/pg_dump"
            command = f"{pg_dump_path} {connection_string} -f {output_file}"
            print("Выполняем команду dump")
            # Выполняем команду
            subprocess.run(command, shell=True, check=True)
        except subprocess.CalledProcessError as e:
            return None, f"Ошибка при создании дампа: {e}"
        return output_file, None