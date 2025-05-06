# -*- coding: UTF-8 -*-

from common.OracleHandle import OracleHandle
from service.oracle_meta.oracle_meta_abstract import OracleTableMeta
from service.oracle_opt.oracle_common import OracleCommon
import common.utils as utils
import logging
logger = logging.getLogger("")


class OracleMetaHandle:
    """
    Oracle Meta info handle
    """
    def __init__(self, oracle_handle):
        assert isinstance(oracle_handle, OracleHandle)
        if oracle_handle is None:
            raise Exception("oracle connect invalid")

        self._oracle_handle = oracle_handle
        self._oracle_common = OracleCommon(oracle_handle)

    def get_oracle_tab_meta(self, oracle_tab_meta) -> bool:
        """
        获取 OracleTableMeta 基础META
        :param oracle_tab_meta: OracleTableMeta
        :return: bool
        """
        assert isinstance(oracle_tab_meta, OracleTableMeta)

        if utils.str_is_none_or_empty(oracle_tab_meta.physical_conn_url):
            oracle_tab_meta.physical_conn_url = "{0}:{1}/{2}".format(self._oracle_handle.get_address(),
                                                                     self._oracle_handle.get_port(),
                                                                     self._oracle_handle.get_instance_name())

        if utils.str_is_none_or_empty(oracle_tab_meta.instance_name):
            oracle_tab_meta.instance_name = self._oracle_handle.get_instance_name()

        if utils.str_is_none_or_empty(oracle_tab_meta.schema_name):
            msg = "schema name invalid"
            oracle_tab_meta.last_error_info = msg
            return False
        else:
            # format schema name
            oracle_tab_meta.schema_name = oracle_tab_meta.schema_name.upper().strip()

        if utils.str_is_none_or_empty(oracle_tab_meta.table_name):
            msg = "table_name name invalid"
            oracle_tab_meta.last_error_info = msg
            return False
        else:
            # format schema name
            oracle_tab_meta.table_name = oracle_tab_meta.table_name.upper().strip()

        tab_dict = self._oracle_common.get_view_dba_tables(oracle_tab_meta.schema_name, oracle_tab_meta.table_name)
        if tab_dict is None or len(tab_dict) <= 0:
            msg = "[{0}.{1}] get dba_tables method failed [{2}]".\
                format(oracle_tab_meta.schema_name, oracle_tab_meta.table_name, self._oracle_common.last_error_info)
            oracle_tab_meta.last_error_info = msg
            oracle_tab_meta.info_result = False
            oracle_tab_meta.last_error_info = msg
            return False

        oracle_tab_meta.tablespace_name = tab_dict["TABLESPACE_NAME"]
        oracle_tab_meta.table_status = tab_dict["STATUS"]
        oracle_tab_meta.table_numrows = tab_dict["NUM_ROWS"]

        oracle_tab_meta.table_blocks = tab_dict["BLOCKS"]
        oracle_tab_meta.table_avg_row_len = tab_dict["AVG_ROW_LEN"]
        oracle_tab_meta.table_sample_size = tab_dict["SAMPLE_SIZE"]
        oracle_tab_meta.table_last_analyzed = tab_dict["LAST_ANALYZED"]
        oracle_tab_meta.table_partitioned = tab_dict["PARTITIONED"]
        oracle_tab_meta.table_readonly = tab_dict["READ_ONLY"]
        oracle_tab_meta.table_comments = tab_dict["COMMENTS"]

        if oracle_tab_meta.table_partitioned.upper() == "YES":
            part_tab_dict = self._oracle_common.get_view_dba_part_tables(
                oracle_tab_meta.schema_name, oracle_tab_meta.table_name)

            if part_tab_dict is None:
                msg = "[{0}.{1}] partition meta method failed [{2}]".\
                    format(oracle_tab_meta.schema_name, oracle_tab_meta.table_name, self._oracle_common.last_error_info)
                oracle_tab_meta.last_error_info = msg
                oracle_tab_meta.info_result = False
                logger.error("[0] {1}".format(utils.get_current_class_methord_name(self), msg))
                return False

            oracle_tab_meta.table_partition_methord = part_tab_dict["PARTITIONING_TYPE"]
            oracle_tab_meta.table_partition_interval = part_tab_dict["INTERVAL"]
            oracle_tab_meta.table_partition_keys = part_tab_dict["PART_COLUMNS"]

        # handle column
        col_meta_list = self._oracle_common.get_view_tab_columns(
            oracle_tab_meta.schema_name, oracle_tab_meta.table_name)
        if col_meta_list is None:
            msg = "[{0}.{1}] get column meta method failed [{2}]".\
                format(oracle_tab_meta.schema_name, oracle_tab_meta.table_name, self._oracle_common.last_error_info)
            oracle_tab_meta.last_error_info = msg
            oracle_tab_meta.info_result = False
            logger.error("[0] {1}".format(utils.get_current_class_methord_name(self), msg))
            return False
        oracle_tab_meta.table_columns = col_meta_list

        # handle index
        idx_meta_list = self._oracle_common.get_view_dba_index(oracle_tab_meta.schema_name, oracle_tab_meta.table_name)
        if idx_meta_list is None:
            msg = "[{0}.{1}] get index meta method failed [{2}]".\
                format(oracle_tab_meta.schema_name, oracle_tab_meta.table_name, self._oracle_common.last_error_info)
            oracle_tab_meta.last_error_info = msg
            oracle_tab_meta.info_result = False
            logger.error("[0] {1}".format(utils.get_current_class_methord_name(self), msg))
            return False
        oracle_tab_meta.table_indexes = idx_meta_list

        # handle table privs
        tab_privs = self._oracle_common.get_view_dba_tab_privs(oracle_tab_meta.schema_name, oracle_tab_meta.table_name)
        if tab_privs is None:
            msg = "[{0}.{1}] get privs meta method failed [{2}]".\
                format(oracle_tab_meta.schema_name, oracle_tab_meta.table_name, self._oracle_common.last_error_info)
            oracle_tab_meta.last_error_info = msg
            oracle_tab_meta.info_result = False
            logger.error("[0] {1}".format(utils.get_current_class_methord_name(self), msg))
            return False
        oracle_tab_meta.table_privileges = tab_privs

        oracle_tab_meta.info_result = True
        return True
