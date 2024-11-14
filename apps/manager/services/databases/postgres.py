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