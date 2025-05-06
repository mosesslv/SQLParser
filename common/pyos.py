# -*- coding: utf-8 -*-

import os
import sys
import subprocess
import platform
import getpass
import socket
import paramiko
current_os = sys.platform
co = platform.system()


class Error:
    """
    对错误内容进行说明
    """

    # error信息字典列表 {"error_key":"key", "error_content":"content"}
    error_list = [

    ]

    def __init__(self):
        pass

    def get_error_info(self, error_key):
        """
        获取错误内容
        :param error_key:error_key
        :return:error_content
        """

        error_content = ""
        for errordic in self.error_list:
            if error_key == errordic["error_key"]:
                error_content = errordic["error_content"]
                break
        return error_content


class ProcessInfo:
    """
    进程信息，可在此类扩展返回需求
    """

    # pid
    pid = 0
    # 命令内容
    shell_name = ""
    # 进程句柄
    process = None
    # 执行错误信息
    error_info = ""
    # 进程返回值
    return_code = 0
    # 进程返回内容
    stdout_info = ""

    def __init__(self):
        pass


class Pyos:
    """
    关于 python 和 os 间的操作
    """

    process_info = ProcessInfo()
    return_code = 0

    def __init__(self):
        pass

    def call_os_shell(self, shell_name, shell_stdin=None, shell_stdout=subprocess.PIPE):
        """
        调用系统命令
        :param shell_name:
        :return:
        """

        self.process_info.shell_name = shell_name

        try:
            self.process_info.process = subprocess.Popen(
                shell_name,
                shell=True,
                stdin=shell_stdin,
                stdout=shell_stdout,
                stderr=shell_stdout
            )

            self.process_info.pid = self.process_info.process.pid
            # self.process_info.process.wait()
            stdoutdata, stderrdata = self.process_info.process.communicate()
            self.process_info.return_code = self.process_info.process.returncode
            self.process_info.stdout_info = stdoutdata
            self.process_info.error_info = stderrdata

            self.return_code = 0

        except OSError as e:
            # 系统异常
            self.process_info.error_info = "[ERROR {0}] {1}".format(e.errno, e.strerror)
            self.return_code = 1
        except KeyboardInterrupt:
            # 键盘中断
            self.process_info.error_info = "exited after ctrl-c"
            try:
                self.process_info.process.terminate()
            except OSError:
                pass
            self.process_info.process.communicate()
            self.return_code = 1
        except Exception as e:
            self.process_info.error_info = "{0}".format(e)
            self.return_code = 1


class PyProcess:
    """
    os 进程的通用方法
    如果是SSH执行, 只支持免密执行
    """
    def __init__(self, ssh_exec=False, ssh_host="", ssh_port=22, ssh_user="", ssh_timeout=10):
        """
        init
        :param ssh_exec: 是否SSH执行
        :param ssh_host: 远程IP
        :param ssh_port: 远程PORT
        :param ssh_user: SSH用户
        :param ssh_timeout: SSH超时时间
        :return:
        """
        self.last_error_info = ""
        self._ssh_exec = ssh_exec
        if self._ssh_exec:
            if len(ssh_host) <= 0 or len(ssh_user) <= 0:
                raise EnvironmentError("ssh exec need remote ip and username")

            if not self.user_no_password_authorized(ssh_host, ssh_user, ssh_port):
                raise EnvironmentError("{0} need password for login".format(ssh_user))

            self._ssh_prefix = "ssh -o StrictHostKeyChecking=no -o ConnectTimeout={0} -o PasswordAuthentication=no" \
                               " {1}@{2} ".format(ssh_timeout, ssh_user, ssh_host)

    def get_process_pid(self, keyword):
        """
        获取指定关键字的PID
        :param keyword:
        :return: list
        """
        pyos = Pyos()
        command = "ps -ef | grep -i {0}| grep -v grep | awk '{{print \\$2}}'".format(keyword)
        if self._ssh_exec:
            exec_command = "{0}\"{1}\"".format(self._ssh_prefix, command)
        else:
            exec_command = command

        pyos.call_os_shell(exec_command)
        if pyos.return_code == 0 and pyos.process_info.return_code == 0:
            pids = pyos.process_info.stdout_info.split("\n")
            return self._list_str_item_clear_empty_line_and_duplication_data(pids)
        else:
            return None

    def get_process_cmd(self, keyword):
        """
        获取指定关键字的cmd
        ps -ef|grep masterha|awk '{for(i=8;i<=NF;i++)printf"%s ",$i;printf"\n"}'

        ssh -o StrictHostKeyChecking=no -o ConnectTimeout=10 -o PasswordAuthentication=no dpaa@172.16.20.106
        "ps -ef | grep -i masterha | grep -v grep |awk '{for(i=8;i<=NF;i++)printf\"%s \",\$i;printf\"\n\"}'"
        :param keyword:
        :return: list
        """
        pyos = Pyos()
        command = "ps -ef | grep -i {0}| grep -v grep | awk '{{for(i=8;i<=NF;i++)printf\\\"%s \\\",\\$i;printf\\\"\\n\\\"}}'".\
            format(keyword)
        if self._ssh_exec:
            exec_command = "{0}\"{1}\"".format(self._ssh_prefix, command)
        else:
            exec_command = command

        pyos.call_os_shell(exec_command)
        if pyos.return_code == 0 and pyos.process_info.return_code == 0:
            cmd_list = pyos.process_info.stdout_info.split("\n")
            return self._list_str_item_clear_empty_line_and_duplication_data(cmd_list)
        else:
            return None

    def kill_pid(self, pids):
        """
        kill process
        :param pids: list - item is int
        :return: boolean
        """
        pid_str = " ".join(map(str, pids))
        pyos = Pyos()
        command = "kill -9 {0}".format(pid_str)
        if self._ssh_exec:
            exec_command = "{0}{1}".format(self._ssh_prefix, command)
        else:
            exec_command = command

        pyos.call_os_shell(exec_command)
        if pyos.return_code == 0 and pyos.process_info.return_code == 0:
            return True
        else:
            return False

    # noinspection PyMethodMayBeStatic
    def _get_current_user_private_file_path(self):
        """
        当前用户ssh id_rsa file path
        :return:
        """
        username = getpass.getuser()
        home_path = os.path.expanduser('~')
        path = "{0}/.ssh/id_rsa".format(home_path)
        return path

    # noinspection PyMethodMayBeStatic
    def _list_str_item_clear_empty_line_and_duplication_data(self, list_obj):
        """
        list 对象, 字符串元素删除空元素, 并滤重
        :param list_obj:
        :return: list
        """
        i = 0
        while i < len(list_obj):
            if list_obj[i] is None or len(str(list_obj[i])) <= 0:
                list_obj.pop(i)
                i -= 1
            i += 1
        obj_list = list(set(list_obj))
        return obj_list

    def user_no_password_authorized(self, ssh_host, ssh_user, ssh_port=22):
        """
        用户免密认证检查
        :param ssh_host:
        :param ssh_user:
        :param ssh_port:
        :return: boolean
        """
        keyfile = self._get_current_user_private_file_path()

        if not (keyfile is not None and len(keyfile) > 0):
            return False

        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((ssh_host, int(ssh_port)))
            t = paramiko.Transport(sock)
        except Exception as e:
            self.last_error_info = str(e)
            return False

        try:
            t.start_client()
        except paramiko.SSHException:
            return False

        try:
            key_file = open(keyfile, 'r')
            key_head = key_file.readline()
            key_file.seek(0)
            if 'DSA' in key_head:
                keytype = paramiko.DSSKey
            elif 'RSA' in key_head:
                keytype = paramiko.RSAKey
            else:
                raise Exception("Can't identify key type")

            pkey = keytype.from_private_key(key_file)
            t.auth_publickey(ssh_user, pkey)
            # t.auth_password(username)
            if t.is_authenticated():
                result = True
            else:
                result = False
        except Exception as e:
            self.last_error_info = str(e)
            result = False
        finally:
            t.close()
        return result


"""
subprocess.wait() 会导致死锁，程序卡在 wait() 调用上;
ulimit -a 看到的 pipe size，那只是每页的大小，linux 默认的 pipe size 是 64KB。
使用communicate()获取返回内容，内容大小取决于内存大小
"""


# demo
if __name__ == "__main__":
    p = Pyos()
    sn = ["watch", "-t", "-d", "-n", "1", "uptime"]
    a = "ls -lrt /tmp"
    p.call_os_shell(a)
    # for line in p.process_info.error_info:
    #     print line

    # for line in p.process_info.stdout_info:
    #     print line
