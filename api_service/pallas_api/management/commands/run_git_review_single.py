from django.core.management.base import BaseCommand
from api_service.pallas_api.utils import exec_task_by_sql_sequence


class Command(BaseCommand):
    help = 'It is a fake command, Import init data for test'

    def add_arguments(self, parser):
        parser.add_argument('-s', '--seq', dest='seq', default=False, help='sqlseq')

    def handle(self, *args, **options):
        seq = options.get('seq')
        if seq:
            print(exec_task_by_sql_sequence(seq))