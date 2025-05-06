from django.core.management.base import BaseCommand

from api_service.pallas_api.utils import u_table_schema_by_profile


class Command(BaseCommand):
    help = 'It is a fake command, Import init data for test'

    def handle(self, *args, **options):
        self.stdout.write(self.style.SUCCESS('begin import'))
        print("Test Command")

        u_table_schema_by_profile('', '', '')

        print("pass")
        self.stdout.write(self.style.SUCCESS("end import"))