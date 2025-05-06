# -*- coding:utf-8 -*-


class OracleTableMeta(object):
    """
    ORACLE TABLE META 抽象信息, 只关注与ORACLE字典相关的内容, 业务相关的内容不再这里定义
    """
    def __init__(self):
        self.info_result = False            # 获取数据时可能发生异常, 通过此变量确认获取是否正常
        self.last_error_info = ""           # 错误内容
        self.physical_conn_url = ""         # 物理信息 xxxx:1521/instance_name
        self.instance_name = ""             # instance name
        self.schema_name = ""               # schema name  => 必填
        self.table_name = ""                # table name   => 必填
        self.tablespace_name = ""           # dba_tables -> tablespace_name
        self.table_status = ""              # dba_tables -> status
        self.table_numrows = 0              # dba_tables -> NUM_ROWS
        self.table_blocks = 0               # dba_tables -> BLOCKS
        self.table_avg_row_len = 0          # dba_tables -> AVG_ROW_LEN
        self.table_sample_size = 0          # dba_tables -> SAMPLE_SIZE
        self.table_last_analyzed = ""       # dba_tables -> LAST_ANALYZED
        self.table_partitioned = ""         # dba_tables -> PARTITIONED
        self.table_readonly = ""            # dba_tables -> READ_ONLY
        self.table_comments = ""            # comments

        self.table_partition_methord = ""   # partition methord -> dba_part_tables.PARTITIONING_TYPE
        self.table_partition_keys = []      # partition column for list obj
        self.table_partition_interval = ""  # interval -> dba_part_tables.INTERVAL
        self.table_columns = []             # table columns list :
                                            # {
                                            #     "OWNER": str(owner),
                                            #     "TABLE_NAME": str(table_name),
                                            #     "COLUMN_NAME": str(column_name),
                                            #     "DATA_TYPE": "" if data_type is None else str(data_type),
                                            #     "DATA_LENGTH": -1 if data_length is None else int(data_length),
                                            #     "DATA_PRECISION": xx,
                                            #     "DATA_SCALE": -1 if data_scale is None else int(data_scale),
                                            #     "NULLABLE": "" if nullable is None else str(nullable),
                                            #     "COLUMN_ID": -1 if column_id is None else int(column_id),
                                            #     "DEFAULT_LENGTH": xx,
                                            #     "DATA_DEFAULT": "" if data_default is None else str(data_default),
                                            #     "NUM_DISTINCT": -1 if num_distinct is None else int(num_distinct),
                                            #     "DENSITY": -1 if density is None else float(density),
                                            #     "NUM_NULLS": -1 if num_nulls is None else int(num_nulls),
                                            #     "NUM_BUCKETS": -1 if num_buckets is None else int(num_buckets),
                                            #     "LAST_ANALYZED": "" if last_analyzed is None else str(last_analyzed),
                                            #     "SAMPLE_SIZE": -1 if sample_size is None else int(sample_size),
                                            #     "HISTOGRAM": "" if histogram is None else str(histogram),
                                            #     "COMMENTS": "" if comments is None else str(comments)
                                            # }
        self.table_indexes = []             # table index list :
                                            # {
                                            #     "OWNER": str(idx_owner),
                                            #     "INDEX_NAME": str(idx_index_name),
                                            #     "INDEX_TYPE": str(idx_index_type),
                                            #     "TABLE_OWNER": str(idx_table_owner),
                                            #     "TABLE_NAME": str(idx_table_name),
                                            #     "UNIQUENESS": str(idx_uniqueness),
                                            #     "TABLESPACE_NAME": str(idx_tablespace_name),
                                            #     "BLEVEL": int(idx_blevel),
                                            #     "LEAF_BLOCKS": int(idx_leaf_blocks),
                                            #     "DISTINCT_KEYS": int(idx_distinct_keys),
                                            #     "STATUS": str(idx_status),
                                            #     "NUM_ROWS": int(idx_num_rows),
                                            #     "SAMPLE_SIZE": int(idx_sample_size),
                                            #     "LAST_ANALYZED": str(idx_last_analyzed),
                                            #     "PARTITIONED": str(idx_partitioned),
                                            #     "CONSTRAINT_NAME": str(idx_constraint_name),
                                            #     "CONSTRAINT_TYPE": str(idx_constraint_type),
                                            #     "INDEX_COLUMNS": idx_cols -> list
                                            # }
        self.table_privileges = []          # table special grant list :{'priv_grantee':'','priv_privilege':''}


class OracleTableStatistics:
    """
    关于表的统计数据
    """
    def __init__(self):
        super(OracleTableStatistics, self).__init__()
        self.info_result = False            # 获取数据时可能发生异常, 通过此变量确认获取是否正常
        self.last_error_info = ""           # 错误内容
        self.table_name = ""                # table name   => 必填
        self.is_need_meta = False           # 是否需要TABLE元数据
        self.table_meta = None              # is_need_meta = True 时为OracleTableMeta
        self.table_size = 0                 # table size(bytes)


class LufaxOracleTableMeta(OracleTableMeta):
    """
    LUFAX META
    """
    def __init__(self):
        super(LufaxOracleTableMeta, self).__init__()
        self.table_seq_name = ""            # table sequence name
        self.tab_synonym_created = 0        # table synonym created
        self.seq_synonym_created = 0        # sequence synonym created
        self.opr_select_granted = 0         # opr user select granted
        self.opr_insert_granted = 0         # opr user insert granted
        self.opr_update_granted = 0         # opr user update granted
        self.rnd_select_granted = 0         # rnd user select granted
        self.seq_opr_select_granted = 0     # opr user sqeuqnce select granted (for oracle)
        self.schema_group = ()              # schema group: 元组数据类型(xxxDATA, xxxOPR, xxxRND),
                                            # 标识非规范的schema group; 例如:B系统; 一般情况不用填写,
                                            # 用于查找OPR,RND权限时需要
        self.usage = ""                     # 用途, 区分下游使用方式, 对应不同的使用处理模块  => 必填
