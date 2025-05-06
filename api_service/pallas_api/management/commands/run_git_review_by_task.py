from django.core.management.base import BaseCommand

from api_service.pallas_api.models import AiSrTaskSql
from api_service.pallas_api.utils import exec_task_by_sql_sequence


class Command(BaseCommand):
    help = 'It is a fake command, Import init data for test'

    def add_arguments(self, parser):
        parser.add_argument('-t', '--task_id', dest='task_id', default=False, help='task_id')

    def handle(self, *args, **options):
        task_id = options.get('task_id')
        asts = AiSrTaskSql.objects.filter(task_id=task_id).all()
        for a in asts:
            print('#' * 100)
            print('seq 为 %s ' % a.as_dict())
            print(exec_task_by_sql_sequence(a.sql_sequence))
            print('#' * 100)
