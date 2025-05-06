# Create your tasks here
from __future__ import absolute_import, unicode_literals

import logging
from datetime import datetime

import celery
from celery import shared_task

from api_service.pallas_api.models import AiSrTask, AiSrReviewRequest
from api_service.pallas_api.utils import exec_task_by_git, exec_task_by_task_info

logger = logging.getLogger(__name__)


class MyTask(celery.Task):
    # 任务失败时执行
    def on_failure(self, exc, task_id, args, kwargs, einfo):
        print('{0!r} failed: {1!r}'.format(task_id, exc))
        logging.error('{0!r} failed: {1!r}, kwargs:{2}, einfo: {3}'.format(task_id, exc, kwargs, einfo))

    # 任务成功时执行
    def on_success(self, retval, task_id, args, kwargs):
        print('success')
        pass

    # 任务重试时执行
    def on_retry(self, exc, task_id, args, kwargs, einfo):
        pass


# @celery.task(base=MyTask)
@shared_task(base=MyTask)
def add(x, y):
    raise Exception('xxxx')
    # return x + y


@celery.task(base=MyTask)
def query():
    a = AiSrTask.objects.all().first()
    return a.as_dict()


@shared_task
def mul(x, y):
    return x * y


# @shared_task
# def async_exec_task_by_sql_list(task_id, sql_list, username, tenant):
#
#     return exec_task_by_sql_list(task_id, sql_list, username, tenant)


# @shared_task(queue='default', base=MyTask)
# def async_exec_task_by_git(app_name, repo_url, task_id, tag, username, tenant):
#     try:
#         task_list = exec_task_by_git(app_name, repo_url, task_id, tag, username, tenant)
#         for task_info in task_list:
#             async_exec_task_by_task_info.apply_async(kwargs=task_info)
#     except Exception as e:
#         AiSrTask.objects.filter(task_id=task_id).update(task_status="FAILED", task_message=str(e),
#                                                         updated_at=datetime.now())
#         AiSrReviewRequest.objects.filter(task_id=task_id).update(review_status="FAIL", comments=str(e),
#                                                                  updated_at=datetime.now())
#         logger.exception('pallas api async_exec_task_by_git error, %s' % e)
#     return {'app_name': app_name, 'repo_url': repo_url, 'task_id': task_id}


@shared_task(queue='default', base=MyTask)
def async_exec_task_by_git(app_name, repo_url, task_id, tag, username, tenant):
    try:
        task_list = exec_task_by_git(app_name, repo_url, task_id, tag, username, tenant)

        index = 0
        while True:
            if len(task_list[index * 100: (index + 1) * 100]) == 0:
                break
            async_exec_task_by_git_part.apply_async(args=(task_id, task_list[index * 100: (index + 1) * 100]))
            index += 1

    except Exception as e:
        AiSrTask.objects.filter(task_id=task_id).update(task_status="FAILED", task_message=str(e),
                                                        updated_at=datetime.now())
        AiSrReviewRequest.objects.filter(task_id=task_id).update(review_status="FAIL", comments=str(e),
                                                                 updated_at=datetime.now())
        logger.exception('pallas api async_exec_task_by_git error, %s' % e)
    return {'app_name': app_name, 'repo_url': repo_url, 'task_id': task_id}


@shared_task(queue='default', base=MyTask)
def async_exec_task_by_git_part(task_id, task_list):
    try:
        for task_info in task_list:
            async_exec_task_by_task_info.apply_async(kwargs=task_info)
    except Exception as e:
        AiSrTask.objects.filter(task_id=task_id).update(task_status="FAILED", task_message=str(e),
                                                        updated_at=datetime.now())
        AiSrReviewRequest.objects.filter(task_id=task_id).update(review_status="FAIL", comments=str(e),
                                                                 updated_at=datetime.now())
        logger.exception('pallas api async_exec_task_by_git error, %s' % e)


@shared_task(queue='review', base=MyTask)
def async_exec_task_by_task_info(**kwargs):
    # exec_task_by_task_info
    return exec_task_by_task_info(**kwargs)

