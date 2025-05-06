import cx_Oracle  # 引用模块cx_Oracle
from django.core.management.base import BaseCommand


class Command(BaseCommand):
    help = 'It is a fake command, Import init data for test'

    def handle(self, *args, **options):
        self.stdout.write(self.style.SUCCESS('begin import'))
        print("Test Command")

        conn = cx_Oracle.connect('system/oracle123@172.168.71.55:1521/lumeta')  # 连接数据库
        c = conn.cursor()  # 获取cursor
        sys_user = "'SYS', 'SYSTEM', 'OUTLN', 'MGMT_VIEW', 'FLOWS_FILES', 'MDSYS', 'ORDSYS', 'EXFSYS', 'DBSNMP', 'WMSYS', 'APPQOSSYS', 'APEX_030200', 'OWBSYS_AUDIT', 'ORDDATA', 'CTXSYS', 'ANONYMOUS', 'SYSMAN', 'XDB', 'ORDPLUGINS', 'OWBSYS', 'SI_INFORMTN_SCHEMA', 'OLAPSYS', 'SCOTT', 'ORACLE_OCM', 'XS$NULL', 'MDDATA', 'DIP', 'APEX_PUBLIC_USER', 'SPATIAL_CSW_ADMIN_USR', 'SPATIAL_WFS_ADMIN_USR'"

        sql_text = """
        select owner,table_name from dba_tab_privs where grantee='{user}' and privilege='SELECT' and owner not in ({sys_user})
            union all
            select OWNER,table_name from dba_tables where owner='{user}' and owner not in ({sys_user})
            union all
            select owner,table_name from role_tab_privs where role in (with data as (
            select t2.GRANTED_ROLE as role_cas,t1.GRANTED_ROLE from (select grantee,GRANTED_ROLE from dba_role_privs where grantee='{user}' and GRANTED_ROLE not in ('CONNECT','RESOURCE','DBA')) t1 left join dba_role_privs t2 on t2.grantee =t1.GRANTED_ROLE where t1.grantee='{user}')
            select data.role_cas from data union all select GRANTED_ROLE from data) and privilege='SELECT'
        and owner not in ({sys_user})
        union all
        select table_owner,table_name from dba_synonyms where table_owner not in ({sys_user}) AND owner='PUBLIC'
        """ .format(sys_user=sys_user, user='SYSTEM')
        x = c.execute(sql_text)
        ret = x.fetchall()
        d = {}
        for x, y in ret:
            d.setdefault(x, []).append(y)
        schema_info = {}
        for k, v in d.items():
            schema_info[k] = list(set(v))
        print(d.keys())
        c.close()  # 关闭cursor
        conn.close()  # 关闭连接
        print("pass")
        self.stdout.write(self.style.SUCCESS("end import"))


def _ora_connect(self):
    """
    oracle connect
    :return:
    """
    connect_start_time = datetime_to_timestamp(datetime.datetime.now())
    try:
        ora_url = "{0}:{1}/{2}".format(self._db_address, self._db_port, self._instance_name)
        conn = cx_Oracle.Connection(self._username, self._passwd, ora_url, events=True)
        connect_end_time = datetime_to_timestamp(datetime.datetime.now())
        conn.ping()
        self._connect_time = connect_end_time - connect_start_time
        return conn
    except Exception as e:
        logger.error("oracle connect error: {0}".format(e))
        self.last_error_message = "oracle connect error: {0}".format(e)
        raise