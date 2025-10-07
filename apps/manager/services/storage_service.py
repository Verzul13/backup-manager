import boto3
import yadisk
from botocore.exceptions import NoCredentialsError, PartialCredentialsError


class S3StorageSerivce:
    def __init__(self, storage_instance):
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
            key = f'dumps/{operation_id}.{fileformat}'
            self.s3.upload_file(filepath, self.storage_instance.bucket_name, key)
            s3_file_path = key
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

    def download_dump(self, s3_file_path):
        filename = s3_file_path.split("/")[-1]
        local_filepath = f"/tmp/{filename}"
        try:
            self._connect()
            self.s3.download_file(
                Bucket=self.storage_instance.bucket_name,
                Key=s3_file_path,
                Filename=local_filepath
            )
        except getattr(self.s3, "exceptions", object()).__dict__.get("NoSuchKey", Exception) as _:  # noqa
            return None, "File not found in S3"
        except (NoCredentialsError, PartialCredentialsError):
            return None, "Credentials are not valid"
        except Exception as e:
            return None, str(e)
        return local_filepath, None


class YandexDiskStorageSerivce:
    """
    Использует secret_key как OAuth-токен.
    Кладём в /dumps/<operation_id>.<ext>
    """

    def __init__(self, storage_instance):
        self.storage_instance = storage_instance
        if not self.storage_instance.secret_key:
            raise RuntimeError("Yandex Disk OAuth token is empty (use secret_key)")
        self._y = yadisk.YaDisk(token=self.storage_instance.secret_key)

    def upload_dump(self, filepath, operation_id):
        error = None
        remote_path = None
        fileformat = filepath.split(".")[-1]
        try:
            base = "/dumps"
            if not self._y.exists(base):
                self._y.mkdir(base)
            remote_path = f"{base}/{operation_id}.{fileformat}"
            self._y.upload(filepath, remote_path)
        except FileNotFoundError:
            error = "File not found"
        except Exception as e:
            error = str(e)
        return remote_path, error

    def delete_dump(self, filepath):
        try:
            if self._y.exists(filepath):
                self._y.remove(filepath, permanently=True)
            return True
        except Exception:
            return False

    def download_dump(self, remote_path):
        filename = remote_path.split("/")[-1]
        local_filepath = f"/tmp/{filename}"
        try:
            if not self._y.exists(remote_path):
                return None, "File not found in Yandex Disk"
            self._y.download(remote_path, local_filepath)
        except Exception as e:
            return None, str(e)
        return local_filepath, None
