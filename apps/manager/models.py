import uuid

from django.db import models
from django.utils.translation import gettext as _

from manager.choices import DBType, DumpTaskPeriodsChoices, DumpOperationStatusChoices


class AbstractBaseModel(models.Model):
    # Fields
    id = models.CharField(primary_key=True, default=uuid.uuid4, editable=False, max_length=100, db_index=True)
    created_dt = models.DateTimeField(_('Date of creation'), auto_now_add=True, editable=False)
    updated_dt = models.DateTimeField(_('Date of update'), auto_now=True, editable=True)

    class Meta:
        abstract = True


class S3FileStorage(AbstractBaseModel):
    #Fields
    name = models.CharField(_("Name"), max_length=100)
    host = models.URLField(_("Host(https://host/"), max_length=300)
    access_key = models.CharField(_("Access Key"), max_length=300)
    secret_key = models.CharField(_("Secret Key"), max_length=300)
    bucket_name = models.CharField(_("Bucket Name"), max_length=300, default='')

    def __str__(self):
        return self.name

    class Meta:
        verbose_name = _('S3 File Storage')
        verbose_name_plural = _('S3 File Storages')


class UserDatabase(AbstractBaseModel):
    name = models.CharField(_("Name"), max_length=100)
    db_type = models.IntegerField(_("Type"), choices=DBType.choices)
    connection_string = models.TextField(_("Connection Link"))

    def __str__(self):
        return self.name

    class Meta:
        verbose_name = _('Database')
        verbose_name_plural = _('Databases')


class DumpTask(AbstractBaseModel):
    # Relations
    database = models.ForeignKey("manager.UserDatabase", on_delete=models.CASCADE)
    file_storage = models.ForeignKey("manager.S3FileStorage", on_delete=models.CASCADE)

    # Fields
    task_period = models.IntegerField(_("Task Period"), choices=DumpTaskPeriodsChoices.choices)
    max_dumpfiles_keep = models.PositiveIntegerField(_("Max Dump files count to keep"), default=1)

    def __str__(self):
        return str(self.id)

    class Meta:
        verbose_name = _('Dump Task')
        verbose_name_plural = _('Dump Tasks')


class DumpTaskOperation(AbstractBaseModel):
    # Relations
    task = models.ForeignKey("manager.DumpTask", on_delete=models.CASCADE)

    # Fields
    status = models.IntegerField(_("Status"), choices=DumpOperationStatusChoices.choices, default=DumpOperationStatusChoices.CREATED)
    error_text = models.TextField(_("Error text"), blank=True, default=None, null=True)
    dump_path = models.CharField(_("Dump File Path"), max_length=250, null=True, blank=True, default=None)

    def __str__(self):
        return str(self.id)

    class Meta:
        verbose_name = _('Dump Task Operation')
        verbose_name_plural = _('Dump Tasks Operations')


class RecoverBackupOperation(AbstractBaseModel):
    # Relations
    dump_operation = models.ForeignKey("manager.DumpTaskOperation", on_delete=models.CASCADE)

    # Fields
    status = models.IntegerField(_("Status"), choices=DumpOperationStatusChoices.choices, default=DumpOperationStatusChoices.CREATED)
    error_text = models.TextField(_("Error text"), blank=True, default=None, null=True)

    def __str__(self):
        return str(self.id)

    class Meta:
        verbose_name = _('Recover Backup Operation')
        verbose_name_plural = _('Recover Backup Operations')
