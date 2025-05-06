import uuid

import coreapi
import coreschema
from django.contrib.auth.hashers import make_password
from django.contrib.auth.models import AbstractUser
from django.db import models


# Create your models here.


class CustomQuerySetManager(models.Manager):
    """A re-usable Manager to access a custom QuerySet"""

    def __init__(self, *args, **kwargs):
        super(CustomQuerySetManager, self).__init__(*args, **kwargs)

    def get_queryset(self):
        """
        在这里处理一下QuerySet, 然后返回没被标记位delete_mark的QuerySet
        """
        kwargs = {'model': self.model, 'using': self._db}
        if hasattr(self, '_hints'):
            kwargs['hints'] = self._hints
        return self._queryset_class(**kwargs).filter(delete_mark='1')
        # return self._queryset_class(**kwargs)

    def get_by_natural_key(self, username):
        return self.get(**{self.model.USERNAME_FIELD: username})

    def create_superuser(self, username, email, password,  **extra_fields):
        # extra_fields.setdefault('is_staff', True)
        # extra_fields.setdefault('is_superuser', True)
        #
        # if extra_fields.get('is_staff') is not True:
        #     raise ValueError('Superuser must have is_staff=True.')
        # if extra_fields.get('is_superuser') is not True:
        #     raise ValueError('Superuser must have is_superuser=True.')

        return self._create_user(username, email, password, **extra_fields)

    def _create_user(self, username, email, password, **extra_fields):
        """
        Create and save a user with the given username, email, and password.
        """
        if not username:
            raise ValueError('The given username must be set')
        user = self.model(username=username, email=email, **extra_fields)
        user.tenant = 'pallas'
        user.role = 'admin'
        user.password = make_password(password, None, 'pbkdf2_sha256')
        user.save(using=self._db)
        return user


class BaseModel(models.Model):
    created_by = models.CharField(max_length=128, default='pallas', null=True, help_text='创建者')
    created_at = models.DateTimeField(auto_now_add=True, help_text='创建时间')
    updated_by = models.CharField(max_length=128, default='pallas', null=True, help_text='更新时间')
    updated_at = models.DateTimeField(auto_now=True, help_text='更新者')

    class Meta:
        abstract = True
        ordering = ['-created_at']

    objects = CustomQuerySetManager()

    # check_password = check_password

    def as_dict(self):
        return {c.name: getattr(self, c.name, None) if not isinstance(c, (models.DateTimeField, models.ForeignKey)) else str(
            getattr(self, c.name, None)) for c in self._meta.fields}

    def create(self, **kwargs):
        for k, v in kwargs.items():
            if hasattr(self, k):
                setattr(self, k, v)
        self.save()
        return self

    def update(self, **kwargs):
        for k, v in kwargs.items():
            if hasattr(self, k) or k not in ["id", "created_at", "updated_at"]:
                setattr(self, k, v)
        self.save()
        return self

    def get_fields(self, location='query'):
        fields = []
        for c in self._meta.fields:
            fields.append(coreapi.Field(c.name, location=location, schema=coreschema.String(description=c.help_text)))
        return fields


class AiSrSqlDetail(models.Model):
    created_by = models.CharField(max_length=128, null=True)
    created_at = models.DateTimeField(auto_now_add=True, null=True)
    updated_by = models.CharField(max_length=128, null=True)
    updated_at = models.DateTimeField(auto_now=True, null=True)

    sequence = models.CharField(unique=True, max_length=128)
    tenant_code = models.CharField(max_length=128)
    db_type = models.CharField(max_length=128)
    db_conn_url = models.CharField(max_length=512, blank=True, null=True)
    schema_name = models.CharField(max_length=128)
    sql_text = models.TextField()
    statement = models.CharField(max_length=128, blank=True, null=True)
    dynamic_mosaicking = models.CharField(max_length=128, blank=True, null=True)
    table_names = models.CharField(max_length=4096, blank=True, null=True)
    plan_text = models.TextField(blank=True, null=True)
    plan_raw = models.TextField(blank=True, null=True)
    sql_data = models.TextField(blank=True, null=True)
    ai_result = models.TextField(blank=True, null=True)
    message = models.TextField()
    ai_error_code = models.CharField(max_length=128, blank=True, null=True)
    ai_error_type = models.CharField(max_length=128, blank=True, null=True)
    ai_program_type = models.TextField(help_text="")
    opt_sql_text = models.TextField(help_text="")
    opt_plan_text = models.TextField(help_text="")

    class Meta:
        managed = False
        db_table = 'ai_sr_sql_detail'
        ordering = ['-created_at']

    def as_dict(self):
        return {c.name: getattr(self, c.name, None) if not isinstance(c, models.DateTimeField) else str(
            getattr(self, c.name, None)) for c in self._meta.fields}


class AiSrTask(BaseModel):
    delete_mark = models.CharField(max_length=1, default='1', help_text='逻辑删除键')
    tenant = models.CharField(max_length=128, blank=True, null=True)
    userid = models.CharField(max_length=128, blank=True, null=True)
    task_id = models.CharField(max_length=128, null=True)
    task_type = models.CharField(max_length=128, help_text='')
    task_status = models.CharField(max_length=128)
    task_message = models.TextField(blank=True, null=True)
    comment = models.TextField(blank=True, null=True)
    sql_total = models.CharField(max_length=128, blank=True, null=True)
    sql_need_optimize = models.CharField(max_length=128, blank=True, null=True, help_text='待优化总数')
    app_name = models.CharField(max_length=128, blank=True, null=True, help_text='应用名')
    base_tag = models.CharField(max_length=128, blank=True, null=True, help_text='基准版本')
    current_tag = models.CharField(max_length=128, blank=True, null=True, help_text='当前版本')
    tag_review_result = models.CharField(max_length=32, blank=True, null=True, help_text='审核结果')
    valid = models.CharField(max_length=1, default='1', blank=True, null=True, help_text='1为有效； 0为无效')

    def __str__(self):
        return self.task_id

    class Meta:
        managed = False
        db_table = 'ai_sr_task'
        ordering = ['-created_at']


class AiSrTaskSql(BaseModel):
    delete_mark = models.CharField(max_length=1, default='1')
    task_id = models.CharField(max_length=128)
    task_type = models.CharField(max_length=128)
    sql_id_alias = models.CharField(max_length=128)
    sql_sequence = models.CharField(unique=True, max_length=128)
    db_type = models.CharField(max_length=128)
    schema_name = models.CharField(max_length=128)
    sql_text = models.TextField()
    plan_text = models.TextField()
    ref_id = models.CharField(max_length=128, blank=True, null=True)
    review_status = models.CharField(max_length=32, default='0', null=True)
    ai_result = models.CharField(max_length=128, blank=True, null=True)
    ai_recommend = models.TextField(blank=True, null=True)
    ai_message = models.TextField(blank=True, null=True)
    ai_error_code = models.CharField(max_length=128, blank=True, null=True)
    ai_error_type = models.CharField(max_length=128, blank=True, null=True)
    opt_sql_text = models.TextField()
    opt_plan_text = models.TextField()
    ai_program_type = models.CharField(max_length=128, blank=True, null=True)
    sql_md5 = models.CharField(max_length=32, blank=True, null=True)
    valid = models.CharField(max_length=1, default='1', blank=True, null=True)
    review_compare = models.CharField(max_length=32, blank=True, null=True)
    sql_review_suggest = models.TextField()
    review_mark = models.CharField(max_length=1, default='0', blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'ai_sr_task_sql'
        ordering = ['-created_at']


class AiSrProfileManage(BaseModel):
    delete_mark = models.CharField(max_length=1, default='1')
    userid = models.CharField(max_length=128)
    tenant = models.CharField(max_length=128)
    profile_name = models.CharField(max_length=128)
    db_type = models.CharField(max_length=128)
    host = models.CharField(max_length=256, blank=True, null=True)
    port = models.CharField(max_length=256, blank=True, null=True)
    instance_name = models.CharField(max_length=256, blank=True, null=True)
    username = models.CharField(max_length=256, blank=True, null=True)
    passwd = models.CharField(max_length=256, blank=True, null=True)
    description = models.CharField(max_length=512, blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'ai_sr_profile_manage'
        unique_together = (('tenant', 'profile_name'),)


class AiSrTenantSchema(BaseModel):
    delete_mark = models.CharField(max_length=1, default='1')
    userid = models.CharField(max_length=128)
    tenant = models.CharField(max_length=128)
    profile_name = models.CharField(max_length=128)
    db_type = models.CharField(max_length=128)
    schema_name = models.CharField(max_length=128)

    class Meta:
        managed = False
        db_table = 'ai_sr_tenant_schema'
        ordering = ['-created_at']


class AiSrUser(BaseModel):
    delete_mark = models.CharField(max_length=1, default='1')
    userid = models.CharField(max_length=255, blank=True, null=True)
    tenant = models.CharField(max_length=255, blank=True, null=True)
    username = models.CharField(max_length=32, unique=True, primary_key=True)
    password = models.CharField(max_length=255, blank=True, null=True)
    department = models.CharField(max_length=255, blank=True, null=True)
    role = models.CharField(max_length=255, blank=True, null=True)
    email = models.CharField(max_length=255, blank=True, null=True)
    username_chn = models.CharField(max_length=255, blank=True, null=True)
    show_count = models.IntegerField(blank=True, null=True)

    EMAIL_FIELD = 'email'
    USERNAME_FIELD = 'username'
    REQUIRED_FIELDS = ['email']

    class Meta:
        managed = True
        db_table = 'ai_sr_user'
        ordering = ['-created_at']

    @property
    def is_anonymous(self):
        return True

    @property
    def is_authenticated(self):
        return False

    def get_username(self):
        return self.username


class AiSrSchemaTable(BaseModel):
    delete_mark = models.CharField(max_length=1, default='1')
    userid = models.CharField(max_length=128)
    tenant = models.CharField(max_length=128)
    schema_id = models.IntegerField()
    table_name = models.CharField(max_length=128)

    class Meta:
        managed = False
        db_table = 'ai_sr_schema_table'
        unique_together = (('userid', 'tenant', 'schema_id', 'table_name'),)
        ordering = ['-created_at']


class AiSrAppSqlmap(BaseModel):
    delete_mark = models.CharField(max_length=1, default='1')
    review_request_id = models.BigIntegerField()
    version = models.CharField(max_length=100, blank=True, null=True)
    version_type = models.CharField(max_length=100, blank=True, null=True)
    filepath = models.CharField(max_length=200, blank=True, null=True)
    md5 = models.CharField(max_length=100, blank=True, null=True)
    content = models.TextField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'ai_sr_app_sqlmap'
        ordering = ['-created_at']


class AiSrReviewDetail(BaseModel):
    delete_mark = models.CharField(max_length=1, default='1')
    review_request_id = models.BigIntegerField()
    namespace = models.CharField(max_length=200)
    sqlmap_id = models.CharField(max_length=200)
    sql_modified_type = models.CharField(max_length=100)
    sql_old_text = models.TextField(blank=True, null=True)
    sql_new_text = models.TextField()
    plan_old_text = models.TextField(blank=True, null=True)
    plan_new_text = models.TextField(blank=True, null=True)
    sqlmap_files = models.TextField(blank=True, null=True)
    review_status = models.CharField(max_length=50, blank=True, null=True)
    review_comment = models.TextField(blank=True, null=True)
    sub_code = models.CharField(max_length=100, blank=True, null=True)
    hint_status = models.IntegerField(blank=True, null=True)
    white_list = models.CharField(max_length=10, blank=True, null=True)
    dynamic_sql = models.IntegerField(blank=True, null=True)
    tip_code = models.CharField(max_length=100, blank=True, null=True)
    base_plan = models.CharField(max_length=10, blank=True, null=True)
    table_cnt = models.BigIntegerField(blank=True, null=True)
    table_names = models.CharField(max_length=2048, blank=True, null=True)
    order_by = models.CharField(max_length=10, blank=True, null=True)
    db_type = models.CharField(max_length=50, blank=True, null=True)
    ai_result = models.CharField(max_length=128, blank=True, null=True)
    sequence = models.CharField(max_length=128, blank=True, null=True)
    ai_recommend = models.TextField(blank=True, null=True)
    message = models.TextField(blank=True, null=True)
    sql_correct = models.CharField(max_length=10, blank=True, null=True)
    dev_confirm = models.CharField(max_length=10, blank=True, null=True)
    schema_name = models.CharField(max_length=50, blank=True, null=True)
    sql_md5 = models.CharField(max_length=50, blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'ai_sr_review_detail'
        ordering = ['-created_at']


class AiSrReviewRequest(BaseModel):
    delete_mark = models.CharField(max_length=1, default='1')
    task_id = models.CharField(max_length=128)
    app_name = models.CharField(max_length=50, blank=True, null=True)
    repo_path = models.CharField(max_length=1000, blank=True, null=True)
    review_type = models.CharField(max_length=100)
    review_status = models.CharField(max_length=100)
    older_version = models.CharField(max_length=100, blank=True, null=True)
    newer_version = models.CharField(max_length=100, blank=True, null=True)
    comments = models.TextField(blank=True, null=True)
    all_sqlmap_md5sum = models.CharField(max_length=100, blank=True, null=True)
    allow_review = models.CharField(max_length=30, blank=True, null=True)
    allowed_dba = models.CharField(max_length=100, blank=True, null=True)
    old_request_id = models.IntegerField(blank=True, null=True)
    sql_cnt = models.IntegerField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'ai_sr_review_request'
        ordering = ['-created_at']


class AiSrAppRepository(BaseModel):
    delete_mark = models.CharField(max_length=1, default='1')
    app_name = models.CharField(unique=True, max_length=64, blank=True, null=True)
    repo_url = models.CharField(max_length=255, blank=True, null=True)
    is_auth = models.CharField(max_length=1, blank=True, null=True)
    username = models.CharField(max_length=64, blank=True, null=True)
    password = models.CharField(max_length=128, blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'ai_sr_app_repository'
        ordering = ['-created_at']


class SrCheckTagTask(models.Model):
    created_by = models.CharField(max_length=30)
    created_at = models.DateTimeField()
    updated_by = models.CharField(max_length=30)
    updated_at = models.DateTimeField()
    app_name = models.CharField(max_length=250, blank=True, null=True)
    tag = models.CharField(max_length=20, blank=True, null=True)
    all_sqlmap_md5sum = models.CharField(max_length=100, blank=True, null=True)
    status = models.CharField(max_length=250, blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'sr_check_tag_task'
        unique_together = (('app_name', 'tag'),)
        ordering = ['-created_at']


class AiSrCheckHistory(BaseModel):
    delete_mark = models.CharField(max_length=1, default='1')
    app_name = models.CharField(max_length=250, blank=True, null=True)
    tag = models.CharField(max_length=20, blank=True, null=True)
    task_id = models.CharField(max_length=100, blank=True, null=True)
    result = models.CharField(max_length=250, blank=True, null=True)
    msg = models.CharField(max_length=512, blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'ai_sr_check_history'
        ordering = ['-created_at']

