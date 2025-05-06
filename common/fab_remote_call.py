# -*- coding: utf-8 -*-

from fabric.api import *
import fabric.exceptions
import sys
import time
import inspect
import os
import threading
import logging
logger = logging.getLogger("")


def get_current_class_methord_name(obj):
    """
    当前的类和方法名
    class_name.methord_name
    :param obj:
    :return: str
    """
    if obj is not None:
        return "{0}.{1}".format(obj.__class__.__name__, inspect.stack()[1][3])
    else:
        return inspect.stack()[1][3]


class FabCall:
    """
    use fabric package call remote command
    """
    def __init__(self, hosts_list, env_user):
        assert isinstance(hosts_list, list)
        self._remote_call_result = ""                   # remote call result
        self._host_list = hosts_list
        self._env_user = env_user

    def _remote_shell(self, command, quiet=True):
        """
        远程系统调用
        :param command:
        :return:
        """
        result = run(command, quiet=quiet)
        self._remote_call_result = result

    def set_hosts_list(self, hosts_list):
        """
        change hosts list
        :param hosts_list:
        :return:
        """
        assert isinstance(hosts_list, list)
        self._host_list = hosts_list

    def set_env_user(self, env_user):
        """
        change env user
        :param env_user:
        :return:
        """
        self._env_user = env_user

    def exec_command(self, command):
        """
        GET FILE LIST
        :return: LIST
        """
        env.user = self._env_user
        try:
            execute(self._remote_shell, command, hosts=self._host_list, quiet=False)
            return True, self._remote_call_result
        except Exception as e:
            msg = "fabric execute command exception [{0}], error: {1}".format(
                command, e)
            logger.error(msg)
            return False, ""

    @parallel
    def _remote_exec_command(self, command, quiet=True):
        """
        parallel call command
        :param command:
        :return:
        """
        result = run(command, quiet=quiet)
        self._remote_call_result = result

    def parallel_exec_command(self, command):
        """
        parallel call command
        :param command:
        :return:
        """
        env.user = self._env_user
        try:
            execute(self._remote_exec_command, command, hosts=self._host_list, quiet=False)
            return True, self._remote_call_result
        except Exception as e:
            msg = "fabric parallel execute command exception [{0}], error: {1}".format(
                command, e)
            logger.error(msg)
            return False, ""


class stdoutfile(object):
    """
    设备句柄, 同时输出到屏幕和
    """
    console = None
    fhandle = None

    def __init__(self, fhandle):
        self.console = sys.stdout
        self.fhandle = fhandle

    def write(self, string):
        self.console.write(string)
        if not self.fhandle.closed:
            self.fhandle.write(string)

    def reset(self):
        sys.stdout = self.console

    def flush(self):
        self.fhandle.flush()


class Fab:
    """
    use fabric package call remote command
    """
    def __init__(self, hosts_list=None, env_user=None, log_file=None, connect_timeout=10, command_timeout=20):
        self._host_list = hosts_list
        self._env_user = env_user
        self._log_file = log_file
        self._connect_timeout = connect_timeout     # 远程连接超时时间
        self._command_timeout = command_timeout     # 命令超时时间
        # self._file_lock = "UNLOCK"  # 文件锁, 这里使用线程实时刷新文件, 锁保证资源争用问题
        # self._file_handle = None    # LOF FILE HANDLE
        # self._stdout_file = None    # 设备句柄

        # self._remote_call_response = False
        # self._remote_call_result = ""                   # remote call result

    def set_host_list(self, host_list):
        self._host_list = host_list

    def set_username(self, username):
        self._env_user = username

    def set_log_file(self, log_file):
        """
        change env user
        :param log_file:
        :return:
        """
        self._log_file = log_file

    @serial
    def _remote_serial_exec_command(self, command, quiet=False, shell=True, log_file=None):
        """
        serial exec command
        :param command:
        :param quiet:
        :param log_file:
        :return:
        """
        file_handle = None
        # env.warn_only = True

        def _thread_flush_file(file_handle):
            logger.info("{0} file flush thread start ......".format(log_file))
            while not file_handle.closed:
                try:
                    file_handle.flush()
                except:
                    continue

            logger.info("{0} file closed".format(log_file))

        # 指定了文件句柄, 需要将内容实时发送文件; 否则只打印终端
        if log_file is not None and len(log_file) > 0:
            real_log_file = "{0}.{1}".format(log_file, env.host_string)
            file_handle = open(real_log_file, 'w')
            t = threading.Thread(target=_thread_flush_file, args=(file_handle,))
            t.start()

            stdout_file = stdoutfile(file_handle)
        else:
            real_log_file = None
            stdout_file = sys.stdout

        try:
            # result = run("{0}; echo \n".format(command), quiet=quiet, stdout=stdout_file)
            result = run("{0}".format(command), shell=shell, quiet=quiet, stdout=stdout_file)
            if real_log_file is not None and len(real_log_file) > 0:
                while True:
                    time.sleep(0.5)
                    try:
                        file_handle.close()
                        break
                    except:
                        continue
            return True, result
        except SystemExit:
            msg = "[{0}] system error exit".format(command)
            logger.error(msg)
            return False, msg
        except fabric.exceptions.CommandTimeout:
            msg = "[{0}] fabric serial execute command timeout".format(command)
            logger.error(msg)
            return False, msg
        except Exception as ex:
            msg = "[{0}] fabric serial execute command exception [{1}]".format(command, ex)
            logger.error(msg)
            return False, msg

    def serial_exec_command(self, command, quiet=True, shell=True):
        """
        serial call command
        :param command:
        :return:
        """
        try:
            env.user = self._env_user
            env.always_use_pty = False
            env.timeout = self._connect_timeout
            env.command_timeout = self._command_timeout

            # env.warn_only = True
            result = execute(self._remote_serial_exec_command, command, hosts=self._host_list,
                             quiet=quiet, shell=shell, log_file=self._log_file)
            return True, result
        except Exception as ex:
            msg = "[{0}] fabric serial execute command exception [{1}]".format(command, ex)
            logger.error(msg)
            return False, {}

    @parallel
    def _remote_parallel_exec_command(self, command, quiet=False, shell=True, log_file=None):
        """
        parallel call command
        :param command:
        :return:
        """
        # thread flush file , file lock
        # _file_lock = "UNLOCK"
        file_handle = None
        env.warn_only = True

        def _thread_flush_file(file_handle):

            logger.info("{0} file flush thread start ......".format(log_file))
            while not file_handle.closed:
                # time.sleep(0.1)
                # if *file_lock == "UNLOCK":
                #     _file_lock = "LOCK"
                #     file_handle.flush()
                #     _file_lock = "UNLOCK"
                try:
                    file_handle.flush()
                except:
                    continue

            logger.info("{0} file closed".format(log_file))

        # 指定了文件句柄, 需要将内容实时发送文件; 否则只打印终端
        if log_file is not None and len(log_file) > 0:
            real_log_file = "{0}.{1}".format(log_file, env.host_string)
            file_handle = open(real_log_file, 'w')
            t = threading.Thread(target=_thread_flush_file, args=(file_handle,))
            t.start()

            stdout_file = stdoutfile(file_handle)
        else:
            real_log_file = None
            stdout_file = sys.stdout

        try:
            result = run(command, quiet=quiet, shell=shell, stdout=stdout_file)

            if real_log_file is not None and len(real_log_file) > 0:
                while True:
                    time.sleep(0.5)
                    try:
                        file_handle.close()
                        break
                    except:
                        continue

                    # if self._file_lock == "UNLOCK":
                    #     self._file_lock = "LOCK"
                    #     file_handle.close()
                    #     time.sleep(0.5)
                    #     break

            return True, result
        except SystemExit:
            msg = "[{0}] system error exit".format(command)
            logger.error(msg)
            return False, msg
        except fabric.exceptions.CommandTimeout:
            msg = "[{0}] fabric serial execute command timeout".format(command)
            logger.error(msg)
            return False, msg
        except Exception as ex:
            msg = "[{0}] fabric serial execute command exception [{1}]".format(command, ex)
            logger.error(msg)
            return False, msg

    def parallel_exec_command(self, command, quiet=False, shell=True):
        """
        parallel call command
        :param command:
        :return:
        """
        env.user = self._env_user
        env.always_use_pty = False
        env.warn_only = True
        env.timeout = self._connect_timeout
        env.command_timeout = self._command_timeout

        # if self._log_file is not None and len(self._log_file) > 0:
        #     self._file_handle = open(self._log_file, "w")
        # self._stdout_file = stdoutfile(self._file_handle)
        # self._start_file_flush_thread()

        try:
            result = execute(self._remote_parallel_exec_command, command, log_file=self._log_file,
                             hosts=self._host_list, quiet=quiet)
            return True, result
        except Exception as ex:
            msg = "[{0}] fabric parallel execute command exception [{1}]".format(command, ex)
            logger.error(msg)
            return False, ""

    # noinspection PyMethodMayBeStatic
    def local_exec_command(self, command, quiet=True, shell=None, log_file=None):
        """
        local call command
        :param command:
        :return:
        """
        try:
            env.abort_exception = Exception
            if log_file is not None and len(log_file) > 0:
                real_command = "{0} >> {1}".format(command, log_file)
            else:
                real_command = command
            result = local(real_command, capture=quiet, shell=shell)
            return True, result
        except Exception as e:
            msg = "[{0}] exec command exception, error: {1}".format(command, e)
            logger.error(msg)
            return False, msg


class OsCommon:
    """
    OS 基础命令工具及功能
    """
    def __init__(self, host=None, env_user="root", connect_timeout=10, command_timeout=20):
        """
        init
        :param host: None - local exec
        :param env_user: os exec user
        :param connect_timeout:
        :param command_timeout:
        :return:
        """
        self._host = host
        self._is_remote = True if host is not None and len(host) > 0 else False
        self._env_user = env_user
        self._connect_timeout = connect_timeout
        self._command_timeout = command_timeout
        self._fab_obj = Fab(hosts_list=[self._host], env_user=self._env_user,
                            connect_timeout=self._connect_timeout, command_timeout=self._command_timeout)

    def directory_is_exists(self, dir_path):
        """
        目录是否存在
        :param dir_path:
        :return: boolean
        """
        if self._is_remote:
            command = "test -d {0}; echo $?".format(dir_path)
            dir_command_result, dir_command_content = self._fab_obj.serial_exec_command(command)
            if not dir_command_result:
                msg = "[{0}] fab exec failed".format(command)
                logger.error("[{0}] {1}".format(get_current_class_methord_name(self), msg))
                return False

            host_result, host_stdout = dir_command_content[self._host]
            if not host_result:
                msg = "[{0}] command exec failed".format(command)
                logger.error("[{0}] {1}".format(get_current_class_methord_name(self), msg))
                return False

            lines = host_stdout.replace("\r", "").split("\n")
            last_line = lines[-1].strip()
            try:
                last_result = int(last_line)
                if last_result == 0:
                    return True
                else:
                    return False
            except:
                return False

        else:
            return os.path.isdir(dir_path) and os.path.exists(dir_path)


if __name__ == '__main__':
    # hosts = ['172.19.14.39', '172.19.14.40']
    # hosts = ['172.19.14.39']
    # fc = FabCall(hosts, "wls81opr", logfile="/tmp/shm.log")
    # fc = Fab(hosts, "wls81opr", log_file="/tmp/shm.log")
    # fc = Fab(hosts, "wls81opr")
    # res, content = fc.local_exec_command("ifconfig; exit 1", quiet=True, log_file="/tmp/shm2.log")

    hosts = ["172.16.20.106"]
    fab = Fab(hosts, "dpaa")
    # res, content = fab.parallel_exec_command("/usr/local/bin/masterha_check_repl1 --conf=/etc/mha/app4.cnf", quiet=True)
    res, content = fab.parallel_exec_command("ls /tmp/123", quiet=True)
    print(content["172.16.20.106"][1])
