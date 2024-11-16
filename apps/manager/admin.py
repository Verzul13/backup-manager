import subprocess

from django.contrib import admin, messages
from django.contrib.auth.admin import UserAdmin as BaseUserAdmin
from django.contrib.auth.admin import GroupAdmin as BaseGroupAdmin
from django.contrib.auth.models import User, Group
from django.utils.translation import gettext as _
from django.http import HttpRequest

from unfold.admin import ModelAdmin
from unfold.decorators import action

import boto3
from botocore.exceptions import ClientError

from manager.models import S3FileStorage, UserDatabase, DumpTask, DumpTaskOperation, RecoverBackupOperation
from manager.services.databases import DB_INTERFACE 

admin.site.unregister(User)
admin.site.unregister(Group)


@admin.register(User)
class UserAdmin(BaseUserAdmin, ModelAdmin):
    pass


@admin.register(Group)
class GroupAdmin(BaseGroupAdmin, ModelAdmin):
    pass


@admin.register(S3FileStorage)
class S3FileStorageAdmin(ModelAdmin):
    compressed_fields = True
    warn_unsaved_form = True
    list_filter_submit = False
    list_fullwidth = False
    list_display = ["name", "host", "bucket_name"]
    actions = ['check_connection']

    @action(description=_("Check connection"))
    def check_connection(self, request: HttpRequest, queryset):
        for storage in queryset:
            try:
                s3_client = boto3.client(
                    's3',
                    endpoint_url=storage.host,
                    aws_access_key_id=storage.access_key,
                    aws_secret_access_key=storage.secret_key,
                    verify=storage.host.startswith("https")
                )

                # Проверяем доступность, сделав простой запрос (например, list_buckets)
                s3_client.list_buckets()
                messages.success(request, f"{storage.name} connection success!")
            except ClientError as e:
                print(f"Connection failed: {e}")
                messages.error(request, f"{storage.name} Connection failed: {e}")
        



@admin.register(UserDatabase)
class UserDatabaseAdmin(ModelAdmin):
    compressed_fields = True
    warn_unsaved_form = True
    list_filter_submit = False
    list_fullwidth = False
    actions = ['check_connection']
    list_display = ["name", "db_type"]

    @action(description=_("Check connection"))
    def check_connection(self, request: HttpRequest, queryset):
        for db in queryset:
            db_interface = DB_INTERFACE[db.db_type]()
            is_connected = db_interface.check_connection(db.connection_string)
            if is_connected:
                messages.success(request, f"{db.name} connection success!")
            else:
                messages.error(request, f"{db.name} Connection failed!")


class DumpTaskOperationInline(admin.TabularInline):
    model = DumpTaskOperation


@admin.register(DumpTask)
class DumpTaskAdmin(ModelAdmin):
    compressed_fields = True
    warn_unsaved_form = True
    list_filter_submit = False
    list_fullwidth = False
    list_display = ["id", "created_dt", "database", "file_storage", "task_period", "max_dumpfiles_keep"]
    actions = ['execute_dump']
    inlines = [DumpTaskOperationInline]

    @action(description=_("Execute dump"))
    def execute_dump(self, request: HttpRequest, queryset):
        for task in queryset:
            db = task.database
            db_interface = DB_INTERFACE[db.db_type]()
            is_connected = db_interface.check_connection(db.connection_string)
            if not is_connected:
                messages.error(request, f"{task.id}: {db.name} Connection failed!")
                continue
            new_operation = DumpTaskOperation.objects.create(
                task=task,
            )
            subprocess.Popen(["python", "manage.py", "dump_operation", str(new_operation.id)])
            messages.success(request, f"{task.id}: Operation of dump created {new_operation.id}")


@admin.register(DumpTaskOperation)
class DumpTaskOperationAdmin(ModelAdmin):
    compressed_fields = True
    warn_unsaved_form = True
    list_filter_submit = False
    list_fullwidth = False
    list_display = ["id", "created_dt", "task__database", "status"]
    actions = ['reexecute_dump']

    @action(description=_("ReExecute dump"))
    def reexecute_dump(self, request: HttpRequest, queryset):
        for operation in queryset:
            subprocess.Popen(["python", "manage.py", "dump_operation", str(operation.id)])



@admin.register(RecoverBackupOperation)
class RecoverBackupOperationAdmin(ModelAdmin):
    compressed_fields = True
    warn_unsaved_form = True
    list_filter_submit = False
    list_fullwidth = False