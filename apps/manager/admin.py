import subprocess

import boto3
import yadisk
from botocore.exceptions import ClientError
from django.contrib import admin, messages
from django.contrib.auth.admin import GroupAdmin as BaseGroupAdmin
from django.contrib.auth.admin import UserAdmin as BaseUserAdmin
from django.contrib.auth.models import Group, User
from django.http import HttpRequest
from django.utils.translation import gettext as _
from manager.models import (DumpTask, DumpTaskOperation, FileStorage,
                            RecoverBackupOperation, UserDatabase)
from manager.services.databases import DB_INTERFACE
from unfold.admin import ModelAdmin
from unfold.decorators import action

admin.site.unregister(User)
admin.site.unregister(Group)


@admin.register(User)
class UserAdmin(BaseUserAdmin, ModelAdmin):
    pass


@admin.register(Group)
class GroupAdmin(BaseGroupAdmin, ModelAdmin):
    pass


@admin.register(FileStorage)
class FileStorageAdmin(ModelAdmin):
    compressed_fields = True
    warn_unsaved_form = True
    list_filter_submit = False
    list_fullwidth = False

    list_display = ["name", "type", "host", "bucket_name"]
    list_filter = ["type"]
    actions = ["check_connection"]

    fieldsets = (
        (_("General"), {
            "fields": ("name", "type", "secret_key"),  # secret_key только здесь!
            "description": _(
                "For Yandex Disk: OAuth token in 'secret_key'. "
                "For S3: use 'secret_key' as S3 Secret Key."
            ),
        }),
        (_("S3 settings"), {
            "fields": ("host", "bucket_name", "access_key"),
            "classes": ("fs-section", "fs-s3"),
        }),
    )

    class Media:
        js = ("admin/js/file_storage_dynamic_unfold.js",)

    def get_form(self, request, obj=None, **kwargs):
        form = super().get_form(request, obj, **kwargs)
        current_type = (request.POST.get("type") or getattr(obj, "type", FileStorage.TYPE_S3)).lower()

        if current_type == FileStorage.TYPE_YADISK:
            # Yandex: host/bucket/access_key не требуем; secret_key обязателен
            for f in ("host", "bucket_name", "access_key"):
                if f in form.base_fields:
                    form.base_fields[f].required = False
            if "secret_key" in form.base_fields:
                form.base_fields["secret_key"].required = True
                form.base_fields["secret_key"].help_text = _("OAuth token for Yandex Disk.")
        else:
            # S3: требуем все
            for f in ("host", "bucket_name", "access_key", "secret_key"):
                if f in form.base_fields:
                    form.base_fields[f].required = True
            if "secret_key" in form.base_fields:
                form.base_fields["secret_key"].help_text = _("S3 Secret Key.")
        return form

    @action(description=_("Check connection"))
    def check_connection(self, request: HttpRequest, queryset):
        for storage in queryset:
            try:
                if storage.type == FileStorage.TYPE_YADISK:
                    if not storage.secret_key:
                        messages.error(request, _(f"{storage.name}: Yandex Disk token is empty (secret_key)."))
                        continue
                    y = yadisk.YaDisk(token=storage.secret_key)
                    if not y.check_token():
                        messages.error(request, _(f"{storage.name}: Yandex Disk token is invalid."))
                        continue
                    y.get_disk_info()
                    messages.success(request, _(f"{storage.name} (Yandex Disk) connection success!"))
                else:
                    s3_client = boto3.client(
                        "s3",
                        endpoint_url=storage.host,
                        aws_access_key_id=storage.access_key,
                        aws_secret_access_key=storage.secret_key,
                        verify=(storage.host or "").startswith("https"),
                    )
                    s3_client.list_buckets()
                    messages.success(request, _(f"{storage.name} (S3) connection success!"))
            except ClientError as e:
                messages.error(request, _(f"{storage.name} Connection failed: {e}"))
            except Exception as e:
                messages.error(request, _(f"{storage.name} Connection failed: {e}"))


@admin.register(UserDatabase)
class UserDatabaseAdmin(ModelAdmin):
    compressed_fields = True
    warn_unsaved_form = True
    list_filter_submit = False
    list_fullwidth = False
    actions = ["check_connection"]
    list_display = ["name", "db_type"]

    @action(description=_("Check connection"))
    def check_connection(self, request: HttpRequest, queryset):
        for db in queryset:
            db_interface = DB_INTERFACE[db.db_type]()
            is_connected = db_interface.check_connection(db.connection_string)
            if is_connected:
                messages.success(request, _(f"{db.name} connection success!"))
            else:
                messages.error(request, _(f"{db.name} Connection failed!"))


class DumpTaskOperationInline(admin.TabularInline):
    model = DumpTaskOperation


@admin.register(DumpTask)
class DumpTaskAdmin(ModelAdmin):
    compressed_fields = True
    warn_unsaved_form = True
    list_filter_submit = False
    list_fullwidth = False
    list_display = ["id", "created_dt", "database",
                    "file_storage", "task_period", "max_dumpfiles_keep"]
    actions = ['execute_dump']
    inlines = [DumpTaskOperationInline]

    @action(description=_("Execute dump"))
    def execute_dump(self, request: HttpRequest, queryset):
        for task in queryset:
            db = task.database
            db_interface = DB_INTERFACE[db.db_type]()
            is_connected = db_interface.check_connection(db.connection_string)
            if not is_connected:
                messages.error(request, _(f"{task.id}: {db.name} Connection failed!"))
                continue
            new_operation = DumpTaskOperation.objects.create(
                task=task,
            )
            subprocess.Popen(
                ["python", "manage.py", "dump_operation", str(new_operation.id)])
            messages.success(request, _(
                f"{task.id}: Operation of dump created {new_operation.id}"))


@admin.register(DumpTaskOperation)
class DumpTaskOperationAdmin(ModelAdmin):
    compressed_fields = True
    warn_unsaved_form = True
    list_filter_submit = False
    list_fullwidth = False
    list_display = ["id", "created_dt", "task__database", "status"]
    actions = ["reexecute_dump", "restore_dump"]

    @action(description=_("ReExecute dump"))
    def reexecute_dump(self, request: HttpRequest, queryset):
        for operation in queryset:
            subprocess.Popen(
                ["python", "manage.py", "dump_operation", str(operation.id)])

    @action(description=_("Restore dump"))
    def restore_dump(self, request: HttpRequest, queryset):
        for operation in queryset:
            new_restore_operation = RecoverBackupOperation.objects.create(
                dump_operation=operation
            )
            subprocess.Popen(
                ["python", "manage.py", "restore_dump", str(new_restore_operation.id)])
            messages.success(request, _(
                f"Operation of restore dump created {new_restore_operation.id}"))


@admin.register(RecoverBackupOperation)
class RecoverBackupOperationAdmin(ModelAdmin):
    compressed_fields = True
    warn_unsaved_form = True
    list_filter_submit = False
    list_fullwidth = False
    actions = ["restore_dump"]
    list_display = ["created_dt", "dump_operation__task__database",
                    "dump_operation__dump_path", "status"]

    @action(description=_("Restore dump"))
    def restore_dump(self, request: HttpRequest, queryset):
        for operation in queryset:
            subprocess.Popen(
                ["python", "manage.py", "restore_dump", str(operation.id)])
