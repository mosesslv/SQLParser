import logging

import cx_Oracle
from mysql import connector

logger = logging.getLogger(__name__)


def get_schema_table_by_oracle(conn_data):
    """

    :param conn_data: username,passwd,host,port,instance_name
    :return: [(schema, table),....]
    """
    sys_user = "'SYS', 'SYSTEM', 'OUTLN', 'MGMT_VIEW', 'FLOWS_FILES', 'MDSYS', 'ORDSYS', 'EXFSYS', 'DBSNMP', 'WMSYS', 'APPQOSSYS', 'APEX_030200', 'OWBSYS_AUDIT', 'ORDDATA', 'CTXSYS', 'ANONYMOUS', 'SYSMAN', 'XDB', 'ORDPLUGINS', 'OWBSYS', 'SI_INFORMTN_SCHEMA', 'OLAPSYS', 'SCOTT', 'ORACLE_OCM', 'XS$NULL', 'MDDATA', 'DIP', 'APEX_PUBLIC_USER', 'SPATIAL_CSW_ADMIN_USR', 'SPATIAL_WFS_ADMIN_USR'"
    conn = cx_Oracle.connect('{username}/{passwd}@{host}:{port}/{instance_name}'.format(**conn_data))
    c = conn.cursor()  # 获取cursor
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
    """.format(sys_user=sys_user, user=conn_data.get('username').upper())
    # sql_text = "select OWNER,table_name from dba_tables where owner='SYSTEM'"
    x = c.execute(sql_text)
    ret = x.fetchall()
    d = {}
    for x, y in ret:
        d.setdefault(x, []).append(y)
    if not ret:
        raise Exception('not found table')
    # map(lambda _d:set(d.values()), d, d)
    schema_info = {}
    for k, v in d.items():
        schema_info[k] = list(set(v))
    c.close()
    conn.close()
    # logging.info("schema_table %s", d)
    return schema_info


def get_schema_table_by_mysql(conn_data):
    """
    all权限
    select concat(user,'@',host),select_priv from mysql.user
    where select_priv='y' and host<>'localhost' and host<>'127.0.0.1' and user='';

    select table_schema,table_name from information_schema.tables
    where table_schema not in ('performance_schema','information_schema','mysql','sys');
    db权限:
    select concat(user,'@',host),db,select_priv from mysql.db
    where select_priv='Y' and host<>'localhost' and host<>'127.0.0.1' and user='';

    select table_schema,table_name from information_schema.tables
    where table_schema not in ('performance_schema','information_schema','mysql','sys') and table_schema='';
    表权限:
    select concat(user,'@',host),db,table_name from mysql.tables_priv
    where table_priv like '%select%' and host<>'localhost' and host<>'127.0.0.1' and user='';
    :param conn_data:
    :return:
    """

    config = {
        'user': conn_data['username'],
        'password': conn_data['passwd'],
        'host': conn_data['host'],
        'database': conn_data['instance_name'],
        'port': conn_data['port'],
        'buffered': True,
    }
    cnx = connector.connect(**config)  # 建立连接
    cursor = cnx.cursor(dictionary=True)

    sql = """
    select concat(user,'@',host),select_priv from mysql.user
    where select_priv='y' and host<>'localhost' and host<>'127.0.0.1' and user='{username}';
    """.format(**conn_data)
    cursor.execute(sql)
    all_permissions = cursor.fetchone()
    print('**'*10, all_permissions)
    ret = []
    if all_permissions:
        sql = """
        select table_schema,table_name from information_schema.tables
        where table_schema not in ('performance_schema','information_schema','mysql','sys');
        """
        cursor.execute(sql)
        # print(cursor.fetchone())
        # print(len(cursor.fetchall()))
        ret = cursor.fetchall()
    else:
        sql = """
        select concat(user,'@',host),db,select_priv from mysql.db
        where select_priv='Y' and host<>'localhost' and host<>'127.0.0.1' and user='{username}';
        """.format(**conn_data)
        cursor.execute(sql)
        db_permissions = cursor.fetchall()
        if db_permissions:
            for item in db_permissions:
                # print(item)
                sql = """
                select table_schema,table_name from information_schema.tables
                where table_schema not in ('performance_schema','information_schema','mysql','sys') and table_schema='{db}';
                """.format(**item)
                cursor.execute(sql)
                ret += cursor.fetchall()
                print(len(cursor.fetchall()))
    d = {}
    for item in ret:
        if item['table_name'] not in d.get(item['table_schema'], []):
            d.setdefault(item['table_schema'], []).append(item['table_name'])
    # print(d.keys())
    # print(d)
    cursor.close()
    cnx.close()
    return d


if __name__ == '__main__':
    get_schema_table_by_mysql({'username': 'dpaacc'})

