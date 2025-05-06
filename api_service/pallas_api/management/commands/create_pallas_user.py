from django.core.management.base import BaseCommand
from django.contrib.auth.hashers import make_password, check_password

import requests


class Command(BaseCommand):
    help = 'It is a fake command, Import init data for test'

    def add_arguments(self, parser):
        parser.add_argument('--user', dest='user',
                            default=False, help='user')

    def handle(self, *args, **options):
        self.stdout.write(self.style.SUCCESS('begin import'))
        password = make_password('admin', None, 'pbkdf2_sha256')
        print("Test Command")

        print(args)

        print("pass")
        self.stdout.write(self.style.SUCCESS("end import"))
