from django.core.management.base import BaseCommand
from django.contrib.auth.hashers import make_password, check_password

import requests

from api_service.pallas_api.models import AiSrAppRepository
from api_service.pallas_api.utils import get_tag_list, switch_tag
from service.GitSQLReview.xml_sql_parser import XmlSqlParser


class Command(BaseCommand):
    help = 'It is a fake command, Import init data for test'

    def add_arguments(self, parser):
        parser.add_argument('-a', '--app', dest='app', default=False, help='app_name')
        parser.add_argument('-t', '--tag', dest='tag', default=False, help='tag')

    def handle(self, *args, **options):
        git_info = AiSrAppRepository.objects.filter(app_name=options.get('app')).first()
        switch_tag(options.get('app'), git_info.app_name, git_info.repo_url)
        xml_parser = XmlSqlParser(file_path=options.get('path'))
        sql_list = xml_parser.get_sql_by_file()
        print(list(sql_list))