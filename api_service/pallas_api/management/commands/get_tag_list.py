from django.core.management.base import BaseCommand
from api_service.pallas_api.models import AiSrAppRepository
from api_service.pallas_api.utils import get_tag_list, exec_task_by_git
from service.GitSQLReview.GitRepository import GitRepository
from service.GitSQLReview.xml_sql_parser import XmlSqlParser


class Command(BaseCommand):
    help = 'It is a fake command, Import init data for test'

    def add_arguments(self, parser):
        parser.add_argument('-a', '--app_name', dest='app_name', default=False, help='app_name')

    def handle(self, *args, **options):
        # git_info = AiSrAppRepository.objects.filter(app_name=options.get('app_name')).first()
        # if git_info:
        pass
        #     # tag_list = get_tag_list(app_name=options.get('app_name'), repo_url=git_info.repo_url)[-200:]
        #     # print(list(map(lambda _d : _d.strip('refs/tags'), filter(lambda _d: 'refs/tags/' in _d, tag_list))))
        # git_info = AiSrAppRepository.objects.filter(app_name='user-app').first()
        # repo_info = GitRepository(app_name=git_info.app_name, repo_url=git_info.repo_url)
        # for item in repo_info.search_files(filter_regex='.xml$'):
        #     xml_parser = XmlSqlParser(file_path=item)
        #     sql_list = xml_parser.get_sql_by_file()
        #     if sql_list:
        #         sql.
        #             for sql in sql_list:
        #                 print(sql.sql_text)
                # print(list(sql_list))
            # 待处理逻辑
        # else:
        #     return []