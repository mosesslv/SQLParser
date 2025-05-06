# -*- coding: UTF-8 -*-

from common.MysqlHandle import MysqlHandle
from service.mysql_meta.mysql_meta_abstract import MysqlTableMeta
from service.mysql_opt.mysql_common import MysqlCommon
import common.utils as utils
import logging
logger = logging.getLogger("")


class MysqlMetaHandle:
    """
    Mysql Meta info handle
    """
    def __init__(self, mysql_handle):
        assert isinstance(mysql_handle, MysqlHandle)
        if mysql_handle is None:
            raise Exception("mysql connect invalid")

        self._mysql_handle = mysql_handle
        self._mysql_common = MysqlCommon(self._mysql_handle)

    def get_mysql_tab_meta(self, mysql_tab_meta) -> bool:
        """
        获取 MysqlTableMeta 基础 META 数据结构
        :param mysql_tab_meta: MysqlTableMeta
        :return: bool
        """
        assert isinstance(mysql_tab_meta, MysqlTableMeta)

        if utils.str_is_none_or_empty(mysql_tab_meta.physical_conn_url):
            mysql_tab_meta.physical_conn_url = "{0}:{1}".\
                format(self._mysql_handle.get_host(), self._mysql_handle.get_port())

        if utils.str_is_none_or_empty(mysql_tab_meta.schema_name):
            msg = "schema name invalid"
            mysql_tab_meta.last_error_info = msg
            return False

        if utils.str_is_none_or_empty(mysql_tab_meta.table_name):
            msg = "table_name name invalid"
            mysql_tab_meta.last_error_info = msg
            return False

        tab_dict = self._mysql_common.get_meta_tables(mysql_tab_meta.schema_name, mysql_tab_meta.table_name)
        if tab_dict is None or len(tab_dict) <= 0:
            msg = "[{0}.{1}] get tables data failed [{2}]".\
                format(mysql_tab_meta.schema_name, mysql_tab_meta.table_name, self._mysql_common.last_error_info)
            mysql_tab_meta.last_error_info = msg
            mysql_tab_meta.info_result = False
            return False

        mysql_tab_meta.table_numrows = -1 if tab_dict["TABLE_ROWS"] is None else int(tab_dict["TABLE_ROWS"])
        mysql_tab_meta.table_avg_row_len = -1 if tab_dict["AVG_ROW_LENGTH"] is None else int(tab_dict["AVG_ROW_LENGTH"])
        mysql_tab_meta.table_update_time = tab_dict["CREATE_TIME"] if tab_dict["UPDATE_TIME"] is None \
            else tab_dict["UPDATE_TIME"]
        mysql_tab_meta.table_comments = "" if utils.str_is_none_or_empty(tab_dict["TABLE_COMMENT"]) is None \
            else str(tab_dict["TABLE_COMMENT"])

        partitions_result = self._mysql_common.get_meta_partition(mysql_tab_meta.schema_name, mysql_tab_meta.table_name)
        if partitions_result is None:
            msg = "[{0}.{1}] get partitions data failed [{2}]". \
                format(mysql_tab_meta.schema_name, mysql_tab_meta.table_name, self._mysql_common.last_error_info)
            mysql_tab_meta.last_error_info = msg
            mysql_tab_meta.info_result = False
            return False

        partition_data = partitions_result[1]
        if len(partition_data) <= 0:
            mysql_tab_meta.table_partitioned = False
            mysql_tab_meta.table_partition_methord = ""
            mysql_tab_meta.table_partition_keys = []
            mysql_tab_meta.table_partition_interval = ""
        else:
            one_partition_data = partition_data[0]
            if one_partition_data[3] is None:   # PARTITION_NAME is None -> No partition table
                mysql_tab_meta.table_partitioned = False
                mysql_tab_meta.table_partition_methord = ""
                mysql_tab_meta.table_partition_keys = []
                mysql_tab_meta.table_partition_interval = ""
            else:
                mysql_tab_meta.table_partitioned = True
                # PARTITION_METHOD
                mysql_tab_meta.table_partition_methord = "" if one_partition_data[7] is None \
                    else str(one_partition_data[7])
                mysql_tab_meta.table_partition_keys = []
                # PARTITION_EXPRESSION
                # mysql_tab_meta.table_partition_interval = "" if one_partition_data[9] is None \
                #     else str(one_partition_data[9])
                mysql_tab_meta.table_partition_interval = ""

        # handle column
        col_meta_result = self._mysql_common.get_meta_columns(mysql_tab_meta.schema_name, mysql_tab_meta.table_name)
        if col_meta_result is None:
            msg = "[{0}.{1}] get column meta method failed [{2}]".\
                format(mysql_tab_meta.schema_name, mysql_tab_meta.table_name, self._mysql_common.last_error_info)
            mysql_tab_meta.last_error_info = msg
            mysql_tab_meta.info_result = False
            logger.error("[0] {1}".format(utils.get_current_class_methord_name(self), msg))
            return False

        col_header = col_meta_result[0]
        col_data = col_meta_result[1]
        col_meta_list = []
        for col_row in col_data:
            col_dict = {
                col_header[3]: "" if col_row[3] is None else str(col_row[3]),   # COLUMN_NAME
                col_header[4]: -1 if col_row[4] is None else int(col_row[4]),   # ORDINAL_POSITION
                col_header[5]: "" if col_row[5] is None else str(col_row[5]),   # COLUMN_DEFAULT
                col_header[6]: "" if col_row[6] is None else str(col_row[6]),   # IS_NULLABLE
                col_header[7]: "" if col_row[7] is None else str(col_row[7]),   # DATA_TYPE
                col_header[8]: -1 if col_row[8] is None else int(col_row[8]),   # CHARACTER_MAXIMUM_LENGTH
                col_header[9]: -1 if col_row[9] is None else int(col_row[9]),   # CHARACTER_OCTET_LENGTH
                col_header[10]: -1 if col_row[10] is None else int(col_row[10]),   # NUMERIC_PRECISION
                col_header[11]: -1 if col_row[11] is None else int(col_row[11]),   # NUMERIC_SCALE
                col_header[12]: -1 if col_row[12] is None else int(col_row[12]),   # DATETIME_PRECISION
                col_header[13]: "" if col_row[13] is None else str(col_row[13]),   # CHARACTER_SET_NAME
                col_header[14]: "" if col_row[14] is None else str(col_row[14]),   # COLLATION_NAME
                col_header[15]: "" if col_row[15] is None else str(col_row[15]),   # COLUMN_TYPE
                col_header[16]: "" if col_row[16] is None else str(col_row[16]),   # COLUMN_KEY
                col_header[17]: "" if col_row[17] is None else str(col_row[17]),   # EXTRA
                col_header[18]: "" if col_row[18] is None else str(col_row[18]),   # PRIVILEGES
                col_header[19]: "" if col_row[19] is None else str(col_row[19]),   # COLUMN_COMMENT
            }
            col_meta_list.append(col_dict)

        mysql_tab_meta.table_columns = col_meta_list

        # handle index
        idx_meta_result = self._mysql_common.get_meta_index(mysql_tab_meta.schema_name, mysql_tab_meta.table_name)
        if idx_meta_result is None:
            msg = "[{0}.{1}] get index meta method failed [{2}]".\
                format(idx_meta_result.schema_name, idx_meta_result.table_name, self._mysql_common.last_error_info)
            idx_meta_result.last_error_info = msg
            idx_meta_result.info_result = False
            logger.error("[0] {1}".format(utils.get_current_class_methord_name(self), msg))
            return False

        idx_header = idx_meta_result[0]
        idx_data = idx_meta_result[1]
        idx_meta_list = []
        for idx_row in idx_data:
            idx_dict = {
                idx_header[3]: -1 if idx_row[3] is None else int(idx_row[3]),   # NON_UNIQUE
                idx_header[4]: "" if idx_row[4] is None else str(idx_row[4]),   # INDEX_SCHEMA
                idx_header[5]: "" if idx_row[5] is None else str(idx_row[5]),   # INDEX_NAME
                idx_header[6]: -1 if idx_row[6] is None else int(idx_row[6]),   # SEQ_IN_INDEX
                idx_header[7]: "" if idx_row[7] is None else str(idx_row[7]),   # COLUMN_NAME
                idx_header[8]: "" if idx_row[8] is None else str(idx_row[8]),   # COLLATION
                idx_header[9]: -1 if idx_row[9] is None else int(idx_row[9]),   # CARDINALITY
                idx_header[10]: -1 if idx_row[10] is None else int(idx_row[10]),   # SUB_PART
                idx_header[11]: "" if idx_row[11] is None else str(idx_row[11]),   # PACKED
                idx_header[12]: "" if idx_row[12] is None else str(idx_row[12]),   # NULLABLE
                idx_header[13]: "" if idx_row[13] is None else str(idx_row[13]),   # INDEX_TYPE
                idx_header[14]: "" if idx_row[14] is None else str(idx_row[14]),   # COMMENT
                idx_header[15]: "" if idx_row[15] is None else str(idx_row[15]),   # INDEX_COMMENT
            }
            idx_meta_list.append(idx_dict)

        mysql_tab_meta.table_indexes = idx_meta_list

        mysql_tab_meta.info_result = True
        return True
