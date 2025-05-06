#!/bin/env python
# -*- coding: UTF-8 -*-
'''
此模块作用作用是找出代码中在哪个版本有什么文件

1. 切换版本，悲观锁定代码所在目录
2. 提供文件
3. 提供两个版本之间的差异文件
'''
import os
import re
import sys
import imp
imp.reload(sys)
import common.lock
import sqlreview.settings
# reload(sys)
from subprocess import call, check_call, CalledProcessError
import shlex
import traceback

import logging

logger = logging.getLogger(__name__)

REPO_HOME = "/wls/source"
GIT_BIN = "/usr/bin/env git"

def sync(func):
    """
    串行操作本地 repository
    :param func:
    :return:
    """
    def wrapper(*args, **kv):
        self = args[0]
        print("locked")
        try:
            return func(*args, **kv)
        finally:
            print("unlocked")
    return wrapper


class GitRepository(object):
    """
    直接调用 OS GIT 命令实现的 仓库访问

    一般使用方式：

    1. 初始化实例
    2. checkout("master")
    3. pull()
    4. acquire_lock()
    5. search_files / walk file and dir in repo path
    6. release_lock

    """
    def __init__(self, app_name=None, repo_path=None, repo_url=None):
        self.git_bin_path = GIT_BIN

        self.repo_path = repo_path
        self.repo_url = repo_url
        self.app_name = app_name
        self.repo_path = repo_path
        if repo_path is None and app_name is not None:
            self.repo_path = os.path.join(REPO_HOME, app_name)

        if not os.path.isdir(self.repo_path) or not os.path.exists(os.path.join(self.repo_path, ".git/config")):
            # os.removedirs(self.repo_path)
            # os.makedirs(self.repo_path)
            if repo_url is None:
                raise ValueError("本地 GIT 仓库尚未 clone，初始化，repo_url 不能为空")
            self.clone()
        self.lockname = "repo-lock:{0}:{1}".format(self.app_name, self.repo_path)

        # self.current_path = os.getcwd()

    def run(self, cmd, shell=False, raise_error=False):
        if not shell:
            command = shlex.split(cmd)
        else:
            command = cmd
        if raise_error:
            return check_call(command, shell=shell)
        else:
            return call(command, shell=shell)

    # @property
    # def repository(self):
    #     return self.repo

    # def _cd_repo_directory(self):
    #     if not os.path.isdir(self.repo_path):
    #         self.clone()
    #     os.chdir(self.repo_path)

    # def _go_back_directory(self):
    #     os.chdir(self.current_path)

    def tag_exists(self, tag):
        try:
            logger.info("检查在 Repo {0} 中是否存在 tag {1}".format(self.repo_path, tag))
            self.run(
                '{0} -C {1}  tag | grep {2} '.format(self.git_bin_path, self.repo_path, tag),
                shell=True, raise_error=True)
            return True
        except CalledProcessError:
            logger.warning("tag {0} 不存在".format(tag))
            logger.warning(traceback.format_exc())
            return False

    def tag_list(self):
        try:
            logger.info("获取在 Repo {0} 中所有的tag号 ".format(self.repo_path))
            import subprocess

            st, out = subprocess.getstatusoutput("""{0} -C {1}  for-each-ref --format='%(*creatordate:raw) %(refname) %(*objectname) %(objectname)' refs/tags | sort -n | """.format(self.git_bin_path, self.repo_path) + """grep 'refs/tags/[a-zA-Z0-9_.-]*' -o | sort -t "_" -k 2""")
            # st, out = subprocess.getstatusoutput('{0} -C {1}  tag --sort=taggerdate'.format(self.git_bin_path, self.repo_path))
            return out
        except CalledProcessError:
            logger.warning(traceback.format_exc())
            return ''

    def clone(self):
        logger.info("git clone {0} >>> {1}".format(self.repo_url, self.repo_path))
        # 先清理目录再 git clone
        if os.path.isdir(self.repo_path):
            os.removedirs(self.repo_path)
       #   os.makedirs(self.repo_path)
        self.run("{0} clone {1} {2}".format(self.git_bin_path, self.repo_url, self.repo_path), raise_error=True)

    def pull(self):
        # o.pull(tags=True)
        # self._cd_repo_directory()
        self.run('{0} -C {1}  pull --tags '.format(self.git_bin_path, self.repo_path), raise_error=True)
        # self._go_back_directory()

    def checkout(self, commit_sha_or_tag):
        # self._cd_repo_directory()
        logger.info("{0} checkout to {1}".format(self.repo_path, commit_sha_or_tag))
        self.run('{0}  -C {2} checkout {1} '.format(self.git_bin_path, commit_sha_or_tag, self.repo_path), raise_error=True)
        # self._go_back_directory()

    def _make_filter(self, filter_regex=None):
        if filter_regex is not None:
            if isinstance(filter_regex, str):
                filter = re.compile(filter_regex)
            else:
                filter = filter_regex
        else:
            return re.compile(".*")
        return filter

    def search_files(self, commit_sha_or_tag=None, filter_regex=None):
        # 切换标签
        """
        切换到对应的版本，按照按照文件名筛选
        :param commit_sha_or_tag:
        :param filter_regex:
        """
        if commit_sha_or_tag is not None:
            self.checkout(commit_sha_or_tag)
        filter = self._make_filter(filter_regex)
        for root, dirs, files in os.walk(self.repo_path.strip()):
            if os.path.join(self.repo_path, "build/") in root+"/":
                continue
            if os.path.join(self.repo_path, ".git/") in root+"/":
                continue
            for filename in files:
                if filename.endswith(".class"):
                    continue
                file_full_path = os.path.join(root, filename)
                if filter.search(file_full_path):
                    yield file_full_path

    def acquire_lock(self):
        """
        为保证仓库处的代码是一致的，这里开始串行化
        :return:
        """
        common.lock.acquire(self.lockname, timeout=20)

    def release_lock(self):
        common.lock.release(self.lockname)