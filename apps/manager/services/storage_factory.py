from manager.models import FileStorage

from .storage_service import S3StorageSerivce, YandexDiskStorageSerivce


def get_storage_service(storage_instance: FileStorage):
    if storage_instance.type == FileStorage.TYPE_YADISK:
        return YandexDiskStorageSerivce(storage_instance)
    return S3StorageSerivce(storage_instance)
