# -*- coding: UTF-8 -*-

from common.OracleHandle import OracleHandle, OracleHandleInfo


class OracleAccount:
    """
    处理 ORACLE 连接帐号的相关问题
    """
    def __init__(self, oracle_conn_handle):
        assert isinstance(oracle_conn_handle, OracleHandle)
        self._oracle_conn_handle = oracle_conn_handle
        self.last_error_info = ""

    def check_account_privilege(self) -> bool:
        """
        检查连接帐号相关权限
        :return: bool
        """
