# -*- coding:utf-8 -*-


class TableMeta:
    """
    META 信息, 表的元数据信息都在这里定义
    """
    db_type = ""                    # 数据库类型 ORACLE MYSQL HIVE
    instance_name = ""              # ORACLE - instance name; MYSQL - ip:port;
    schema_name = ""                # schema name
    table_name = ""                 # table name
    table_comments = ""             # comments
    table_seq_name = ""             # table sequence name for oracle
    tab_synonym_created = 0         # table synonym created for oracle
    seq_synonym_created = 0         # sequence synonym created for oracle
    opr_select_granted = 0          # opr user select granted
    opr_insert_granted = 0          # opr user insert granted
    opr_update_granted = 0          # opr user update granted
    rnd_select_granted = 0          # rnd user select granted
    seq_opr_select_granted = 0      # opr user sqeuqnce select granted for oracle
    table_is_partitioned = 0        # table is partitioned
    table_partition_methord = ""    # partition methord
    table_partition_keys = []       # partition columns
    table_partition_interval = ""   # interval
    table_columns = []              # table columns list :
    # {'col_name':'','col_type':'', 'col_null':'', 'default_value':'', 'col_comments':''}
    table_indexes = []              # table index list
    # {'idx_name':'','idx_cols':'', 'idx_part':'' ,'idx_constraint_name':'','idx_constraint_type':''}
    table_privileges = []           # table special grant list :{'priv_grantee':'','priv_privilege':''}

    def __init__(self):
        pass
