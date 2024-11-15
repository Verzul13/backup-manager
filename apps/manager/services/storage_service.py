import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError

from manager.models import S3FileStorage


class StorageServce:
    def __init__(self, storage_instance: S3FileStorage):
        self.storage_instance = storage_instance
        self.s3 = None

    def _connect(self):
        self.s3 = boto3.client(
            's3',
            endpoint_url=self.storage_instance.host,
            aws_access_key_id=self.storage_instance.access_key,
            aws_secret_access_key=self.storage_instance.secret_key,
        )

    def upload_dump(self, filepath, operation_id):
        error = None
        s3_file_path = None
        fileformat = filepath.split(".")[-1]
        try:
            self._connect()
            # Загрузка файла
            self.s3.upload_file(filepath, self.storage_instance.bucket_name, f'dumps/{operation_id}.{fileformat}')
            s3_file_path = f"dumps/{operation_id}.sql"
        except FileNotFoundError:
            error = "File not found"
        except (NoCredentialsError, PartialCredentialsError):
            error = "Credentials are not valid"
        except Exception as e:
            error = str(e)
        return s3_file_path, error

    def delete_dump(self, filepath):
        try:
            self._connect()
            self.s3.delete_object(Bucket=self.storage_instance.bucket_name, Key=filepath)
        except Exception:
            return False
        return True
    
    