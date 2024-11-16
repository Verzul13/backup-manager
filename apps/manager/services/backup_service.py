from manager.models import DumpTaskOperation, RecoverBackupOperation
from manager.choices import DumpOperationStatusChoices
from manager.services.databases import DB_INTERFACE
from manager.services.storage_service import StorageServce



class BackupService:

    def __init__(self, operation_id):
        self.operation_id = operation_id

    def _set_error4operation(self, operation, error):
        print(error)
        operation.status = DumpOperationStatusChoices.FAIL
        operation.error_text = error
        operation.save()

    def make_dump(self):
        operation = DumpTaskOperation.objects.filter(id=self.operation_id).first()
        if not operation:
            return False, f"Operation {self.operation_id} doesn't exist"
        operation.status = DumpOperationStatusChoices.IN_PROCESS
        operation.error_text = None
        operation.save()

        db = operation.task.database
        storage = operation.task.file_storage
        if not storage.bucket_name:
            error = "Need to add bucket name to storage"
            self._set_error4operation(operation, error)
            return False, error
        
        db_interface = DB_INTERFACE[db.db_type]()
        is_connected = db_interface.check_connection(db.connection_string)
        if not is_connected:
            error = "Database connection failed"
            self._set_error4operation(operation, error)
            return False, error
        filepath, error = db_interface.dump_database(db.connection_string, operation.id)
        if error:
            self._set_error4operation(operation, error)
            return False, error
        
        storage_service = StorageServce(storage)
        s3_file_path, error = storage_service.upload_dump(filepath, operation.id)
        if error:
            self._set_error4operation(operation, error)
            return False, error
        print(f"File uploaded successfully to {s3_file_path}")
        operation.status = DumpOperationStatusChoices.SUCCESS
        operation.error_text = None
        operation.dump_path = s3_file_path
        operation.save()

        # max_files2 keep
        previous_dump_operations = DumpTaskOperation.objects.filter(
            task=operation.task,
            status=DumpOperationStatusChoices.SUCCESS,
        ).order_by("-created_dt")
        current_cnt = 0
        max_files_cnt = operation.task.max_dumpfiles_keep
        files2delete = []
        operations2delete = []
        for dump_operation in previous_dump_operations:
            if dump_operation.id == self.operation_id or current_cnt <= max_files_cnt:
                current_cnt += 1
                continue
            files2delete.append(dump_operation.dump_path)
            operations2delete.append(dump_operation.id)
        # delete files
        if files2delete:
            for file_backup_path in files2delete:
                storage_service.delete_dump(file_backup_path)
        if operations2delete:
            DumpTaskOperation.objects.filter(id__in=operations2delete).delete()
        print("Dump Success")

    def restore_dump(self):
        operation = RecoverBackupOperation.objects.filter(id=self.operation_id).first()
        if not operation:
            return False, f"Operation {self.operation_id} doesn't exist"
        operation.status = DumpOperationStatusChoices.IN_PROCESS
        operation.error_text = None
        operation.save()

        dump_operation = operation.dump_operation
        db = dump_operation.task.database
        storage = dump_operation.task.file_storage
        if not storage.bucket_name:
            error = "Need to add bucket name to storage"
            self._set_error4operation(operation, error)
            return False, error
        db_interface = DB_INTERFACE[db.db_type]()
        is_connected = db_interface.check_connection(db.connection_string)
        if not is_connected:
            error = "Database connection failed"
            self._set_error4operation(operation, error)
            return False, error
        
        # DOWNLOAD DUMP
        storage_service = StorageServce(storage)
        filepath, error = storage_service.download_dump(dump_operation.dump_path)
        if error:
            self._set_error4operation(operation, error)
            return False, error
        # RESTORE DUMP
        _, error = db_interface.load_dump(
            filepath=filepath,
            connection_string=db.connection_string
        )
        if error:
            self._set_error4operation(operation, error)
            return False, error

        # Finilize
        print(f"File restored successfully")
        operation.status = DumpOperationStatusChoices.SUCCESS
        operation.error_text = None
        operation.save()
        return True, None
