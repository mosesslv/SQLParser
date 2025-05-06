#!/usr/bin/env python
# encoding: utf-8
# 目的是拒绝隐式引入，celery.py和celery冲突。
from __future__ import absolute_import, unicode_literals
import os, sys
from celery import Celery
from django.conf import settings

# PROJECT_PATH = os.path.dirname('/home/ssm/PycharmProjects/sqlreview/')
# sys.path.append(PROJECT_PATH)
# set the default Django settings module for the 'celery' program.
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'sqlreview.settings')

app = Celery('sqlreview')

# Using a string here means the worker doesn't have to serialize
# the configuration object to child processes.
# - namespace='CELERY' means all celery-related configuration keys
#   should have a `CELERY_` prefix.
app.config_from_object('django.conf:settings')
# app.config_from_object('celery_app.config', namespace='CELERY')

# Load task modules from all registered Django app configs.
# app.autodiscover_tasks(['celery_app'])
app.autodiscover_tasks(lambda: settings.INSTALLED_APPS)
# app.autodiscover_tasks(lambda :settings.INSTALLED_APPS)


@app.task(bind=True)
def debug_task(self):
    print('Request: {0!r}'.format(self.request))

