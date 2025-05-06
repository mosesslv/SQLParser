# -*- coding: UTF-8 -*-

from datetime import datetime
from django.db import models
import django.utils.timezone


class PublicColumns(models.Model):
    # 所有表的公共字段
    id = models.AutoField(primary_key=True)
    created_by = models.CharField(max_length=128, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    created_at.auto_created = True
    updated_by = models.CharField(max_length=128, blank=True)
    updated_at = models.DateTimeField(auto_now=True)
    updated_at.auto_created = True

    class Meta:
        abstract = True


class AISrReviewRequest(PublicColumns):
    task_id = models.CharField(max_length=128, null=False, blank=True)
    app_name = models.CharField(max_length=50, null=True, blank=True)
    repo_path = models.CharField(max_length=1000, null=True, blank=True)
    review_type = models.CharField(max_length=100, null=False, blank=True)
    review_status = models.CharField(max_length=100, null=False, blank=True)
    older_version = models.CharField(max_length=100, null=True, blank=True)
    newer_version = models.CharField(max_length=100, null=True, blank=True)
    comments = models.TextField(help_text="")
    all_sqlmap_md5sum = models.CharField(max_length=100, null=True, blank=True)
    allow_review = models.CharField(max_length=30, null=True, blank=True)
    allowed_dba = models.CharField(max_length=100, null=True, blank=True)
    old_request_id = models.BigIntegerField(null=True, blank=True)
    sql_cnt = models.BigIntegerField(null=True, blank=True)
    delete_mark = models.CharField(max_length=1, default='1')

    class Meta:
        db_table = "ai_sr_review_request"
        verbose_name = 'ai_sr_review_request'
        verbose_name_plural = 'ai_sr_review_request'


class AISrReviewDetail(PublicColumns):
    review_request_id = models.BigIntegerField(null=False, blank=True)
    namespace = models.CharField(max_length=200, null=False, blank=True)
    sqlmap_id = models.CharField(max_length=200, null=False, blank=True)
    sql_modified_type = models.CharField(max_length=100, null=False, blank=True)
    sql_old_text = models.TextField(help_text="")
    sql_new_text = models.TextField(help_text="")
    plan_old_text = models.TextField(help_text="")
    plan_new_text = models.TextField(help_text="")
    sqlmap_files = models.TextField(help_text="")
    review_status = models.CharField(max_length=50, null=True, blank=True)
    review_comment = models.TextField(help_text="")
    sub_code = models.CharField(max_length=100, null=True, blank=True)
    hint_status = models.BigIntegerField(null=True, blank=True)
    white_list = models.CharField(max_length=10, null=True, blank=True)
    dynamic_sql = models.BigIntegerField(null=True, blank=True)
    tip_code = models.CharField(max_length=100, null=True, blank=True)
    base_plan = models.CharField(max_length=10, null=True, blank=True)
    table_cnt = models.BigIntegerField(null=True, blank=True)
    table_names = models.CharField(max_length=2048, null=True, blank=True)
    order_by = models.CharField(max_length=10, null=True, blank=True)
    db_type = models.CharField(max_length=50, null=True, blank=True)
    ai_result = models.CharField(max_length=128, null=True, blank=True)
    sequence = models.CharField(max_length=128, null=True, blank=True)
    ai_recommend = models.TextField(help_text="")
    message = models.TextField(help_text="")
    sql_correct = models.CharField(max_length=10, null=True, blank=True)
    dev_confirm = models.CharField(max_length=10, null=True, blank=True)
    schema_name = models.CharField(max_length=50, null=True, blank=True)
    delete_mark = models.CharField(max_length=1, default='1')

    class Meta:
        db_table = "ai_sr_review_detail"
        verbose_name = 'ai_sr_review_detail'
        verbose_name_plural = 'ai_sr_review_detail'


class AISrAppSqlmap(PublicColumns):
    review_request_id = models.BigIntegerField(null=False, blank=True)
    version = models.CharField(max_length=100, null=False, blank=True)
    version_type = models.CharField(max_length=100, null=False, blank=True)
    filepath = models.CharField(max_length=200, null=False, blank=True)
    md5 = models.CharField(max_length=100, null=True, blank=True)
    content = models.TextField(help_text="")
    delete_mark = models.CharField(max_length=1, default='1')

    class Meta:
        db_table = "ai_sr_app_sqlmap"
        verbose_name = 'ai_sr_app_sqlmap'
        verbose_name_plural = 'ai_sr_app_sqlmap'


class AISrSQLDetail(PublicColumns):
    sequence = models.CharField(max_length=128, null=False, blank=True)
    tenant_code = models.CharField(max_length=128, null=False, blank=True)
    db_type = models.CharField(max_length=128, null=False, blank=True)
    db_conn_url = models.CharField(max_length=512, null=True, blank=True)
    schema_name = models.CharField(max_length=128, null=False, blank=True)
    sql_text = models.TextField(help_text="")
    statement = models.CharField(max_length=128, null=True, blank=True)
    dynamic_mosaicking = models.CharField(max_length=128, null=True, blank=True)
    table_names = models.CharField(max_length=4096, null=True, blank=True)
    plan_text = models.TextField(help_text="")
    plan_raw = models.TextField(help_text="")
    sql_data = models.TextField(help_text="")
    ai_result = models.TextField(help_text="")
    message = models.TextField(help_text="")
    ai_error_code = models.CharField(max_length=128, null=True, blank=True)
    ai_error_type = models.CharField(max_length=128, null=True, blank=True)
    ai_program_type = models.TextField(help_text="")
    opt_sql_text = models.TextField(help_text="")
    opt_plan_text = models.TextField(help_text="")

    class Meta:
        db_table = "ai_sr_sql_detail"
        verbose_name = 'ai_sr_sql_detail'
        verbose_name_plural = 'ai_sr_sql_detail'


class AISrHttpApiLog(PublicColumns):
    ip = models.CharField(max_length=256, null=False, blank=True)
    auth_data = models.CharField(max_length=256, null=True, blank=True)
    request_type = models.CharField(max_length=32, null=True, blank=True)
    request_data = models.TextField(help_text="")
    response_data = models.TextField(help_text="")

    class Meta:
        db_table = "ai_sr_http_request_log"
        verbose_name = "ai_sr_http_request_log"
        verbose_name_plural = "ai_sr_http_request_log"


class AISrProfileManage(PublicColumns):
    userid = models.CharField(max_length=128, null=False, blank=True)
    tenant = models.CharField(max_length=128, null=False, blank=True)
    profile_name = models.CharField(max_length=128, null=False, blank=True)
    db_type = models.CharField(max_length=128, null=False, blank=True)
    host = models.CharField(max_length=256, null=True, blank=True)
    port = models.CharField(max_length=256, null=True, blank=True)
    instance_name = models.CharField(max_length=256, null=True, blank=True)
    username = models.CharField(max_length=256, null=True, blank=True)
    passwd = models.CharField(max_length=256, null=True, blank=True)
    description = models.CharField(max_length=512, null=True, blank=True)
    delete_mark = models.CharField(max_length=1, null=False, blank=True)

    class Meta:
        db_table = "ai_sr_profile_manage"
        verbose_name = "ai_sr_profile_manage"
        verbose_name_plural = "ai_sr_profile_manage"


class AISrTenantSchema(PublicColumns):
    userid = models.CharField(max_length=128, null=False, blank=True)
    tenant = models.CharField(max_length=128, null=False, blank=True)
    profile_name = models.CharField(max_length=128, null=False, blank=True)
    db_type = models.CharField(max_length=128, null=False, blank=True)
    schema_name = models.CharField(max_length=128, null=True, blank=True)
    delete_mark = models.CharField(max_length=1, null=False, blank=True)

    class Meta:
        db_table = "ai_sr_tenant_schema"
        verbose_name = "ai_sr_tenant_schema"
        verbose_name_plural = "ai_sr_tenant_schema"


class AISrTaskSql(PublicColumns):
    task_id = models.CharField(max_length=128, null=False, blank=True)
    task_type = models.CharField(max_length=128, null=False, blank=True)
    sql_id_alias = models.CharField(max_length=128, null=False, blank=True)
    sql_sequence = models.CharField(max_length=128, null=False, blank=True)
    db_type = models.CharField(max_length=128, null=False, blank=True)
    schema_name = models.CharField(max_length=128, null=True, blank=True)
    sql_text = models.TextField(help_text="")
    plan_text = models.TextField(help_text="")
    ref_id = models.CharField(max_length=128, null=True, blank=True)
    review_status = models.CharField(max_length=32, default='0', null=True)
    ai_result = models.CharField(max_length=128, null=True, blank=True)
    ai_recommend = models.TextField(help_text="")
    ai_message = models.TextField(help_text="")
    ai_error_code = models.CharField(max_length=128, null=True, blank=True)
    ai_error_type = models.CharField(max_length=128, null=True, blank=True)
    delete_mark = models.CharField(max_length=1, null=False, blank=True)
    opt_sql_text = models.TextField()
    opt_plan_text = models.TextField()
    ai_program_type = models.CharField(max_length=128, null=False, blank=True)
    valid = models.CharField(max_length=1, default='1')

    class Meta:
        db_table = "ai_sr_task_sql"
        verbose_name = "ai_sr_task_sql"
        verbose_name_plural = "ai_sr_task_sql"


class AISrTask(PublicColumns):
    tenant = models.CharField(max_length=128, null=False, blank=True)
    userid = models.CharField(max_length=128, null=False, blank=True)
    task_id = models.CharField(max_length=128, null=False, blank=True)
    task_type = models.CharField(max_length=128, null=False, blank=True)
    task_status = models.CharField(max_length=128, null=False, blank=True)
    task_message = models.TextField(help_text="")
    comment = models.TextField(help_text="")
    sql_total = models.CharField(max_length=128, null=True, blank=True)
    sql_need_optimize = models.CharField(max_length=128, null=True, blank=True)
    delete_mark = models.CharField(max_length=10, null=True, blank=True)
    app_name = models.CharField(max_length=128, null=True, blank=True)
    base_tag = models.CharField(max_length=128, null=True, blank=True)
    current_tag = models.CharField(max_length=128, null=True, blank=True)
    tag_review_result = models.CharField(max_length=32, null=True, blank=True)
    valid = models.CharField(max_length=1, default='1')

    class Meta:
        db_table = "ai_sr_task"
        verbose_name = "ai_sr_task"
        verbose_name_plural = "ai_sr_task"


# BETTLE临时支持表
class DBATable(PublicColumns):
    _partition_methord_choises = (
        ("", ""),
        ("LIST", "LIST"),
        ("HASH", "HASH"),
        ("RANGE", "RANGE"),
        ("RANGE-YEAR", "RANGE（按年分区）"),
        ("RANGE-MONTH", "RANGE（按月分区）"),
        ("RANGE-DAY", "RANGE（按日分区）"),
        ("SYSTEM", "SYSTEM"),
    )
    schema_group_id = models.BigIntegerField(default=0, help_text="Schema组的ID")
    instance_id = models.BigIntegerField(default=0, help_text="Instance的ID")
    # 原来表的 ID
    source_table_id = models.BigIntegerField(null=True, help_text="表的唯一标识ID")
    name = models.CharField(max_length=30, help_text="表名")
    comments = models.CharField(max_length=4000, help_text="表注释")
    rows_per_month = models.BigIntegerField(default=10000)
    usage = models.CharField(max_length=10, blank=True)
    frequency = models.CharField(max_length=10, blank=True)
    # 该表正被哪个项目修改
    locked_by_proj_id = models.BigIntegerField(default=0, help_text="该表被锁定的项目的ID")
    version = models.BigIntegerField(default=0, help_text="表当前的版本号")
    # 这个表是不是该项目新添加的表
    is_new = models.BooleanField(default=False, help_text="是否在本项目中新建")
    is_dropped = models.BooleanField(default=False)
    # 其他周边的对象的执行情况：
    sequence_created = models.BooleanField(default=False)
    tab_synonym_created = models.BooleanField(default=False)
    seq_synonym_created = models.BooleanField(default=False)

    opr_granted = models.IntegerField(default=0)
    rnd_granted = models.IntegerField(default=0)
    seq_opr_granted = models.IntegerField(default=0)

    # 特殊 sequence 的名字
    sequence_name = models.CharField(blank=True, max_length=30,
                                     help_text="Oracle:sequence的name,留空表示 seq_$table_name； MYSQL，auto_increment 的列名")
    # 分区表支持：
    partition_methord = models.CharField(blank=True, null=True, max_length=30, default="",
                                         choices=_partition_methord_choises, help_text="分区策略")
    partition_key_col = models.CharField(blank=True, null=True, max_length=30, default="PARTITION_KEY",
                                         help_text="分区字段")
    partition_interval = models.CharField(blank=True, null=True, max_length=30, default="1", help_text="分区间隔")
    partition_keep_count = models.IntegerField(blank=True, null=True, help_text="分区保留策略")
    is_split = models.IntegerField(blank=True, null=True, default=0)
    erase_o = models.CharField(blank=True, null=True, max_length=10, default="")

    class Meta:
        db_table = "dba_table"
        verbose_name = "dba_table"
        verbose_name_plural = "dba_table"


class DevTable(PublicColumns):
    _partition_methord_choises = (
        ("", ""),
        ("LIST", "LIST"),
        ("HASH", "HASH"),
        ("RANGE", "RANGE"),
        ("RANGE-YEAR", "RANGE（按年分区）"),
        ("RANGE-MONTH", "RANGE（按月分区）"),
        ("RANGE-DAY", "RANGE（按日分区）"),
        ("SYSTEM", "SYSTEM"),
    )
    schema_group_id = models.BigIntegerField(default=0, help_text="Schema组的ID")
    instance_id = models.BigIntegerField(default=0, help_text="Instance的ID")
    # 原来表的 ID
    source_table_id = models.BigIntegerField(null=True, help_text="表的唯一标识ID")
    name = models.CharField(max_length=30, help_text="表名")
    comments = models.CharField(max_length=4000, help_text="表注释")
    rows_per_month = models.BigIntegerField(default=10000)
    usage = models.CharField(max_length=10, blank=True)
    frequency = models.CharField(max_length=10, blank=True)
    # 该表正被哪个项目修改
    locked_by_proj_id = models.BigIntegerField(default=0, help_text="该表被锁定的项目的ID")
    version = models.BigIntegerField(default=0, help_text="表当前的版本号")
    # 这个表是不是该项目新添加的表
    is_new = models.BooleanField(default=False, help_text="是否在本项目中新建")
    is_dropped = models.BooleanField(default=False)
    # 其他周边的对象的执行情况：
    sequence_created = models.BooleanField(default=False)
    tab_synonym_created = models.BooleanField(default=False)
    seq_synonym_created = models.BooleanField(default=False)

    opr_granted = models.IntegerField(default=0)
    rnd_granted = models.IntegerField(default=0)
    seq_opr_granted = models.IntegerField(default=0)

    # 特殊 sequence 的名字
    sequence_name = models.CharField(blank=True, max_length=30,
                                     help_text="Oracle:sequence的name,留空表示 seq_$table_name； MYSQL，auto_increment 的列名")
    # 分区表支持：
    partition_methord = models.CharField(blank=True, null=True, max_length=30, default="",
                                         choices=_partition_methord_choises, help_text="分区策略")
    partition_key_col = models.CharField(blank=True, null=True, max_length=30, default="PARTITION_KEY",
                                         help_text="分区字段")
    partition_interval = models.CharField(blank=True, null=True, max_length=30, default="1", help_text="分区间隔")
    partition_keep_count = models.IntegerField(blank=True, null=True, help_text="分区保留策略")
    is_split = models.IntegerField(blank=True, null=True, default=0)
    erase_o = models.CharField(blank=True, null=True, max_length=10, default="")

    class Meta:
        db_table = "dev_table"
        verbose_name = "dev_table"
        verbose_name_plural = "dev_table"


class DBAColumn(PublicColumns):
    # 表上的column
    schema_group_id = models.BigIntegerField()
    instance_id = models.BigIntegerField(default=0)
    table_id = models.BigIntegerField(help_text="对应Table的source_table_id，source_table_id 是每个表的唯一编号")
    source_column_id = models.BigIntegerField(null=True, help_text="每个列的唯一编号")
    name = models.CharField(max_length=100, help_text="列名，限制长度为 30 个字符")
    # ref_col = models.CharField(max_length=100, default='')
    col_type = models.CharField(max_length=200, help_text="列类型")
    # 该列是否可以空，存储的值是 not null 或者 null
    col_null = models.CharField(max_length=20, default='', blank=True,
                                help_text="可否为空，NULL/'' 均表示可空， 'NOT NULL'表示不能为空 ")
    default_value = models.CharField(max_length=100, default='', blank=True, help_text="default 值")
    comments = models.CharField(max_length=4000, blank=True, help_text="列的注释 ")
    # 该 column 是不是这个项目新增加上去的，是的话，可以被删除
    is_new = models.BooleanField(default=True, help_text="是否为本项目的新添加的列")
    version = models.BigIntegerField(default=0, help_text="单调递增的版本号 ")

    class Meta:
        db_table = "dba_tab_col"
        verbose_name = "dba_tab_col"
        verbose_name_plural = "dba_tab_col"


class Instance(PublicColumns):
    # 各台服务器的连接信息
    name = models.CharField(max_length=50, help_text="数据库实例名字")
    usage = models.CharField(max_length=10, help_text="用途：DEV/QA/PRD")
    db_type = models.CharField(max_length=10, help_text="ORACLE/MYSQL")
    url = models.CharField(max_length=100, help_text="连接字符串")
    username = models.CharField(max_length=100)
    password = models.CharField(max_length=100)
    dba_username = models.CharField(max_length=100)
    dba_password = models.CharField(max_length=100)

    def __unicode__(self):
        return self.name

    class Meta:
        db_table = "db_instance"
        ordering = ['usage', 'db_type', 'url']
        verbose_name = '数据库实例'
        verbose_name_plural = '(E)数据库实例(instance)'


class SchemaGroup(PublicColumns):
    name = models.CharField(max_length=50, help_text="Schema名字")
    data_user = models.CharField(max_length=50, blank=True, help_text="Schema对应DATA用户")
    data_user_password = models.CharField(max_length=50, blank=True, help_text="Schema对应DATA用户的密码")
    opr_user = models.CharField(max_length=50, blank=True, help_text="Schema对应的OPR用户")
    rnd_user = models.CharField(max_length=50, blank=True, help_text="Schema对应的RND用户")
    instance_type = models.CharField(max_length=16)
    zone = models.CharField(max_length=50, blank=True, help_text="B端/C端")
    # 这类 schema_group 用于管理
    is_managing = models.BooleanField()
    area = models.CharField(max_length=50, blank=True, help_text="所属地区")

    class Meta:
        db_table = "db_schema_group"
        ordering = ['name', 'data_user']
        verbose_name = 'Schema组'
        verbose_name_plural = '(E)Schema组'


class EraseOTableConf(PublicColumns):
    table_name = models.CharField(max_length=128, null=False, blank=True)
    schema_name = models.CharField(max_length=128, null=False, blank=True)
    erase_o_schema_name = models.CharField(max_length=128, null=False, blank=True)
    is_exist_source = models.IntegerField(default=0)
    problem_proj = models.CharField(max_length=512, null=True, blank=True)

    class Meta:
        db_table = "erase_o_table_conf"
        verbose_name = 'erase_o_table_conf'
        verbose_name_plural = 'erase_o_table_conf'


# ------------------------------------ #
#  adds by bohuaijiang, on 2019-12-27  #
# ------------------------------------ #

class AISrTabStrategy(PublicColumns):
    # --- pk index --- #
    instance_name = models.CharField(max_length=128, null=False, blank=True)
    schema_name = models.CharField(max_length=128, null=False, blank=True)
    tab_name = models.CharField(max_length=128, null=False, blank=True)
    # --- #
    columns = models.TextField(help_text="")
    col_distinct = models.TextField(help_text="")
    comb_col_freq = models.TextField(help_text="")
    numrow = models.CharField(max_length=128, null=False, blank=True)
    appearance = models.CharField(max_length=128, null=False, blank=True)
    exist_idx = models.TextField(help_text="")
    sqlid_list = models.TextField(help_text="")

    class Meta:
        db_table = 'ai_sr_tab_strategy'
        verbose_name = 'ai_sr_tab_strategy'
        verbose_name_plural = 'ai_sr_tab_strategy'


class AISrViewedSql(PublicColumns):
    # --- pk index --- #
    sql_id = models.CharField(max_length=128, null=False, blank=True)
    # ---- #
    instance_name = models.CharField(max_length=128, null=False, blank=True)
    schema_name = models.CharField(max_length=128, null=False, blank=True)
    table_names = models.CharField(max_length=4096, null=False, blank=True)
    # ---- #
    sql_text = models.TextField(help_text="")
    apperance = models.FloatField(null=False, blank=True)
    luparser = models.TextField(help_text="")

    class Meta:
        db_table = "ai_sr_viewed_sql"
        verbose_name = "ai_sr_viewed_sql"
        verbose_name_plural = "ai_sr_tab_strategy"


class AISrViewedTab(PublicColumns):
    instance_name = models.CharField(max_length=128, null=False, blank=True)
    schema_name = models.CharField(max_length=128, null=False, blank=True)
    table_names = models.CharField(max_length=4096, null=False, blank=True)

    sql_id = models.CharField(max_length=128, null=False, blank=True)


class AISrOracleSharepoolSrc(PublicColumns):
    instance_url = models.CharField(max_length=128, null=False, blank=True)
    snapshot = models.CharField(max_length=128, null=False, blank=True)
    sql_id = models.CharField(max_length=15, null=False, blank=True)
    sql_text = models.TextField(help_text="")
    sharable_mem = models.IntegerField(default=0)
    persistent_mem = models.IntegerField(default=0)
    runtime_mem = models.IntegerField(default=0)
    sorts = models.IntegerField(default=0)
    loaded_versions = models.IntegerField(default=0)
    open_versions = models.IntegerField(default=0)
    users_opening = models.IntegerField(default=0)
    fetches = models.IntegerField(default=0)
    executions = models.IntegerField(default=0)
    end_of_fetch_count = models.IntegerField(default=0)
    loads = models.IntegerField(default=0)
    first_load_time = models.CharField(max_length=32, null=False, blank=True)
    invalidations = models.IntegerField(default=0)
    parse_calls = models.IntegerField(default=0)
    disk_reads = models.IntegerField(default=0)
    buffer_gets = models.IntegerField(default=0)
    application_wait_time = models.IntegerField(default=0)
    concurrency_wait_time = models.IntegerField(default=0)
    cluster_wait_time = models.IntegerField(default=0)
    user_io_wait_time = models.IntegerField(default=0)
    plsql_exec_time = models.IntegerField(default=0)
    java_exec_time = models.IntegerField(default=0)
    rows_processed = models.IntegerField(default=0)
    command_type = models.IntegerField(default=0)
    optimizer_mode = models.CharField(max_length=32, null=False, blank=True)
    optimizer_cost = models.IntegerField(default=0)
    optimizer_env_hash_value = models.IntegerField(default=0)
    parsing_user_id = models.IntegerField(default=0)
    parsing_schema_id = models.IntegerField(default=0)
    kept_versions = models.IntegerField(default=0)
    hash_value = models.IntegerField(default=0)
    old_hash_value = models.IntegerField(default=0)
    plan_hash_value = models.IntegerField(default=0)
    child_number = models.IntegerField(default=0)
    module = models.CharField(max_length=128, null=False, blank=True)
    module_hash = models.IntegerField(default=0)
    action = models.CharField(max_length=128, null=False, blank=True)
    action_hash = models.IntegerField(default=0)
    serializable_aborts = models.IntegerField(default=0)
    outline_category = models.CharField(max_length=128, null=False, blank=True)
    cpu_time = models.IntegerField(default=0)
    elapsed_time = models.IntegerField(default=0)
    outline_sid = models.IntegerField(default=0)
    sqltype = models.IntegerField(default=0)
    remote = models.CharField(max_length=10, null=False, blank=True)
    object_status = models.CharField(max_length=32, null=False, blank=True)
    literal_hash_value = models.IntegerField(default=0)
    last_load_time = models.CharField(max_length=32, null=False, blank=True)
    is_obsolete = models.CharField(max_length=10, null=False, blank=True)
    child_latch = models.IntegerField(default=0)
    sql_profile = models.CharField(max_length=128, null=False, blank=True)
    program_id = models.IntegerField(default=0)
    parsing_schema_name = models.CharField(max_length=128, null=False, blank=True)
    # --- added by bohuai jiang --- #
    plan_raw_before = models.TextField(help_text="")
    plan_raw_after =models.TextField(help_text="")
    plan_text_before = models.TextField(help_text="")
    plan_text_after = models.TextField(help_text="")
    improvement = models.CharField(max_length=128, null=False, blank=True)

    class Meta:
        db_table = 'ai_sr_oracle_sharepool_src'
        verbose_name = 'ai_sr_oracle_sharepool_src'
        verbose_name_plural = 'ai_sr_oracle_sharepool_src'
