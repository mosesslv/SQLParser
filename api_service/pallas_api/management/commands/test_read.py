import hashlib

from django.core.management.base import BaseCommand
from api_service.pallas_api.utils import switch_tag


class Command(BaseCommand):
    help = 'It is a fake command, Import init data for test'

    def add_arguments(self, parser):
        pass

    def handle(self, *args, **options):
        repo = switch_tag('p2p-report-app', 'git@gitlab.lujs.cn:p2p/p2p-report-app.git', 'reg_20191219_02')
        for file_path in repo.search_files(filter_regex=".xml$"):
            with open(file_path, 'rb') as f:
                contents = f.read()
                md5 = hashlib.md5(contents).hexdigest()
                print(file_path)
                print(md5)
