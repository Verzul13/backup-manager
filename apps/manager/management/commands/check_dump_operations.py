from datetime import datetime

from django.core.management.base import BaseCommand

from manager.models import DumpTask, DumpTaskOperation
from manager.choices import DumpTaskPeriodsChoices
from manager.services.backup_service import BackupService


class Command(BaseCommand):
    help = 'Check and execute dump operations'

    def _process_dump(self, task):
        print("Process task:", task.id)
        new_operation = DumpTaskOperation.objects.create(
            task=task,
        )
        print(f"New operation {task.id} created: ", {new_operation.id})
        backup_service = BackupService(str(new_operation.id))
        backup_service.make_dump()


    def handle(self, *args, **options):
        every_day_tasks = DumpTask.objects.filter(task_period=DumpTaskPeriodsChoices.EVERYDAY)
        for task in every_day_tasks:
            self._process_dump(task)
        today = datetime.now()
        if today.weekday() == 0:
            tasks = DumpTask.objects.filter(task_period=DumpTaskPeriodsChoices.EVERYWEEK)
            for task in tasks:
                self._process_dump(task)
        if today.day == 1:
            tasks = DumpTask.objects.filter(task_period=DumpTaskPeriodsChoices.EVERYMONTH)
            for task in tasks:
                self._process_dump(task)
        print("Check tasks finished")
