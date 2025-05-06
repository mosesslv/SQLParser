# -*- coding:utf-8 -*-


class MysqlTableMeta(object):
    """
    MYSQL TABLE META 抽象信息, 只关注与字典相关的内容, 业务相关的内容不再这里定义
    """
    def __init__(self):
        self.info_result = False            # 获取数据时可能发生异常, 通过此变量确认获取是否正常
        self.last_error_info = ""           # 错误内容
        self.physical_conn_url = ""         # 物理信息 xxxx:1521
        self.schema_name = ""               # schema name  => 必填
        self.table_name = ""                # table name   => 必填
        self.table_numrows = 0              # dba_tables -> NUM_ROWS
        self.table_avg_row_len = 0          # tables -> TABLE_ROWS
        self.table_update_time = ""         # tables -> UPDATE_TIME
        self.table_partitioned = False      # PARTITIONS
        self.table_comments = ""            # comments

        self.table_partition_methord = ""   # partition methord -> dba_part_tables.PARTITIONING_TYPE
        self.table_partition_keys = []      # partition column for list obj
        self.table_partition_interval = ""  # interval -> dba_part_tables.INTERVAL

        self.table_columns = []             # table columns list :
                                            # {
                                            #     "COLUMN_NAME": str(column_name),
                                            #     "ORDINAL_POSITION": position
                                            #     "COLUMN_DEFAULT": default value,
                                            #     "IS_NULLABLE": "" if nullable is None else str(nullable),
                                            #     "DATA_TYPE": "" if data_type is None else str(data_type),
                                            #     "CHARACTER_MAXIMUM_LENGTH": ,
                                            #     "CHARACTER_OCTET_LENGTH": ,
                                            #     "NUMERIC_PRECISION": xx,
                                            #     "NUMERIC_SCALE": ,
                                            #     "DATETIME_PRECISION": xxx
                                            #     "CHARACTER_SET_NAME": ,
                                            #     "COLLATION_NAME": xx,
                                            #     "COLUMN_TYPE": -1 if num_distinct is None else int(num_distinct),
                                            #     "COLUMN_KEY": -1 if density is None else float(density),
                                            #     "EXTRA": -1 if num_nulls is None else int(num_nulls),
                                            #     "PRIVILEGES": -1 if num_buckets is None else int(num_buckets),
                                            #     "COLUMN_COMMENT": "" if comments is None else str(comments)
                                            # }

        self.table_indexes = []             # table index list -> STATISTICS:
                                            # {
                                            #     "NON_UNIQUE": ,
                                            #     "INDEX_SCHEMA": ,
                                            #     "INDEX_NAME": str(idx_index_type),
                                            #     "SEQ_IN_INDEX": str(idx_table_owner),
                                            #     "COLUMN_NAME": str(idx_table_name),
                                            #     "COLLATION": str(idx_uniqueness),
                                            #     "CARDINALITY": str(idx_tablespace_name),
                                            #     "SUB_PART": int(idx_blevel),
                                            #     "PACKED": int(idx_leaf_blocks),
                                            #     "NULLABLE": int(idx_distinct_keys),
                                            #     "INDEX_TYPE": str(idx_status),
                                            #     "COMMENT": int(idx_num_rows),
                                            #     "INDEX_COMMENT": int(idx_sample_size),
                                            # }
