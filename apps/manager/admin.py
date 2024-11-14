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
    # actions_list = ["check_connection"]
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

    @action(description=_("Check connection"))
    def check_connection(self, request: HttpRequest, queryset):
        for db in queryset:
            db_interface = DB_INTERFACE[db.db_type]
            is_connected = db_interface.check_connection(db.connection_string)
            if is_connected:
                messages.success(request, f"{db.name} connection success!")
            else:
                messages.error(request, f"{db.name} Connection failed!")


@admin.register(DumpTask)
class DumpTaskAdmin(ModelAdmin):
    compressed_fields = True
    warn_unsaved_form = True
    list_filter_submit = False
    list_fullwidth = False


@admin.register(DumpTaskOperation)
class DumpTaskOperationAdmin(ModelAdmin):
    compressed_fields = True
    warn_unsaved_form = True
    list_filter_submit = False
    list_fullwidth = False


@admin.register(RecoverBackupOperation)
class RecoverBackupOperationAdmin(ModelAdmin):
    compressed_fields = True
    warn_unsaved_form = True
    list_filter_submit = False
    list_fullwidth = False