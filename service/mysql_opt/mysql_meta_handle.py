# -*- coding:utf-8 -*-
# CREATED BY: bohuai jiang 
# CREATED ON: 
# LAST MODIFIED ON:
# AIM:

from common.MysqlHandle import MysqlHandle
from service.mysql_meta.mysql_meta_abstract import MysqlTableMeta
from service.mysql_opt.mysql_common import MysqlCommon
import common.utils as utils
import logging
logger = logging.getLogger("")


class OracleMetaHandle:
    """
    Oracle Meta info handle
    """
    def __init__(self, mysql_handle):
        assert isinstance(mysql_handle, MysqlHandle)
        if mysql_handle is None:
            raise Exception("oracle connect invalid")

        self._mysql_handle = mysql_handle
        self._mysql_common = MysqlCommon(mysql_handle)

    def get_mysql_tab_meta(self, mysql_tab_meta) -> bool:
        """
        获取 OracleTableMeta 基础META
        :param mysql_tab_meta: OracleTableMeta
        :return: bool
        """
        assert isinstance(mysql_tab_meta, MysqlTableMeta)

        if utils.str_is_none_or_empty(mysql_tab_meta.physical_conn_url):
            mysql_tab_meta.physical_conn_url = "{0}:{1}".format(self._mysql_handle.get_host(),
                                                                     self._mysql_handle.get_port())


        if utils.str_is_none_or_empty(mysql_tab_meta.schema_name):
            msg = "schema name invalid"
            mysql_tab_meta.last_error_info = msg
            return False
        else:
            # format schema name
            mysql_tab_meta.schema_name = mysql_tab_meta.schema_name.upper().strip()

        if utils.str_is_none_or_empty(mysql_tab_meta.table_name):
            msg = "table_name name invalid"
            mysql_tab_meta.last_error_info = msg
            return False
        else:
            # format schema name
            mysql_tab_meta.table_name = mysql_tab_meta.table_name.upper().strip()

        tab_dict = self._mysql_common.get_view_dba_tables(mysql_tab_meta.schema_name, mysql_tab_meta.table_name)
        if tab_dict is None or len(tab_dict) <= 0:
            msg = "[{0}.{1}] get dba_tables method failed [{2}]".\
                format(mysql_tab_meta.schema_name, mysql_tab_meta.table_name, self._mysql_common.last_error_info)
            mysql_tab_meta.last_error_info = msg
            mysql_tab_meta.info_result = False
            logger.error("[0] {1}".format(utils.get_current_class_methord_name(self), msg))
            return False

        mysql_tab_meta.tablespace_name = tab_dict["TABLESPACE_NAME"]
        mysql_tab_meta.table_status = tab_dict["STATUS"]
        mysql_tab_meta.table_numrows = tab_dict["NUM_ROWS"]

        mysql_tab_meta.table_blocks = tab_dict["BLOCKS"]
        mysql_tab_meta.table_avg_row_len = tab_dict["AVG_ROW_LEN"]
        mysql_tab_meta.table_sample_size = tab_dict["SAMPLE_SIZE"]
        mysql_tab_meta.table_last_analyzed = tab_dict["LAST_ANALYZED"]
        mysql_tab_meta.table_partitioned = tab_dict["PARTITIONED"]
        mysql_tab_meta.table_readonly = tab_dict["READ_ONLY"]
        mysql_tab_meta.table_comments = tab_dict["COMMENTS"]

        if mysql_tab_meta.table_partitioned.upper() == "YES":
            part_tab_dict = self._mysql_common.get_view_dba_part_tables(
                mysql_tab_meta.schema_name, mysql_tab_meta.table_name)

            if part_tab_dict is None:
                msg = "[{0}.{1}] partition meta method failed [{2}]".\
                    format(mysql_tab_meta.schema_name, mysql_tab_meta.table_name, self._mysql_common.last_error_info)
                mysql_tab_meta.last_error_info = msg
                mysql_tab_meta.info_result = False
                logger.error("[0] {1}".format(utils.get_current_class_methord_name(self), msg))
                return False

            mysql_tab_meta.table_partition_methord = part_tab_dict["PARTITIONING_TYPE"]
            mysql_tab_meta.table_partition_interval = part_tab_dict["INTERVAL"]
            mysql_tab_meta.table_partition_keys = part_tab_dict["PART_COLUMNS"]

        # handle column
        col_meta_list = self._mysql_common.get_view_tab_columns(
            mysql_tab_meta.schema_name, mysql_tab_meta.table_name)
        if col_meta_list is None:
            msg = "[{0}.{1}] get column meta method failed [{2}]".\
                format(mysql_tab_meta.schema_name, mysql_tab_meta.table_name, self._mysql_common.last_error_info)
            mysql_tab_meta.last_error_info = msg
            mysql_tab_meta.info_result = False
            logger.error("[0] {1}".format(utils.get_current_class_methord_name(self), msg))
            return False
        mysql_tab_meta.table_columns = col_meta_list

        # handle index
        idx_meta_list = self._mysql_common.get_view_dba_index(mysql_tab_meta.schema_name, mysql_tab_meta.table_name)
        if idx_meta_list is None:
            msg = "[{0}.{1}] get index meta method failed [{2}]".\
                format(mysql_tab_meta.schema_name, mysql_tab_meta.table_name, self._mysql_common.last_error_info)
            mysql_tab_meta.last_error_info = msg
            mysql_tab_meta.info_result = False
            logger.error("[0] {1}".format(utils.get_current_class_methord_name(self), msg))
            return False
        mysql_tab_meta.table_indexes = idx_meta_list

        # handle table privs
        tab_privs = self._mysql_common.get_view_dba_tab_privs(mysql_tab_meta.schema_name, mysql_tab_meta.table_name)
        if tab_privs is None:
            msg = "[{0}.{1}] get privs meta method failed [{2}]".\
                format(mysql_tab_meta.schema_name, mysql_tab_meta.table_name, self._mysql_common.last_error_info)
            mysql_tab_meta.last_error_info = msg
            mysql_tab_meta.info_result = False
            logger.error("[0] {1}".format(utils.get_current_class_methord_name(self), msg))
            return False
        mysql_tab_meta.table_privileges = tab_privs

        mysql_tab_meta.info_result = True
        return True
