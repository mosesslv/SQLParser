"""
curl -H "Content-Type: application/json"  -H "AUTHORIZATION: "7584d373e597b0e07bd75501c2adb0aa9e6bd803" -X POST  "http://127.0.0.1:8000/pallas/api/v1/ai_sr_task" -d '{"task_type": "SINGLE", "um": "yangyun620", "tenant": "lufax", "sql_text": "select * from LDG_JOURNAL where posted = \'Dummy\' and RETRY_COUNT = 2 and partition_key <> 10"}'
"""
import copy
from functools import reduce

data = {'task_type': 'SINGLE',
        'um': 'yangyun620',
        'tenant': 'lufax',
        'sql_text': "select * from LDG_JOURNAL where posted = 'Dummy' and RETRY_COUNT = 2 and partition_key <> 10"}
import requests
import json

# ret = requests.post("http://127.0.0.1:8000/pallas/api/v1/ai_sr_task", data=json.dumps(data), headers={"Content-Type": "application/json"})
# print(ret.content)


a = ['ai', 'sr', 'task', 'sql'.capitalize()]
print(list(map(lambda x: x.capitalize(), a)))

print(reduce(lambda x, y: x + y, ['Ai', 'Sr', 'Task', 'Sql']))

"""
from django.contrib.auth.hashers import make_password, check_password

password = make_password('admin', None, 'pbkdf2_sha256')
print(password)
"""

a = {'description': 'test', 'host': '172.168.71.55', 'username': 'system', 'instance_name': 'lumeta',
     'passwd': 'oracle123', 'port': '1521', 'db_type': 'ORACLE', 'profile_name': 'test', 'tenant': 'pallas',
     "created_by": "yangyun620",
     "updated_by": "yangyun620",
     "userid": "yangyun620", }

sys_user = "'SYS', 'SYSTEM', 'OUTLN', 'MGMT_VIEW', 'FLOWS_FILES', 'MDSYS', 'ORDSYS', 'EXFSYS', 'DBSNMP', 'WMSYS', 'APPQOSSYS', 'APEX_030200', 'OWBSYS_AUDIT', 'ORDDATA', 'CTXSYS', 'ANONYMOUS', 'SYSMAN', 'XDB', 'ORDPLUGINS', 'OWBSYS', 'SI_INFORMTN_SCHEMA', 'OLAPSYS', 'SCOTT', 'ORACLE_OCM', 'XS$NULL', 'MDDATA', 'DIP', 'APEX_PUBLIC_USER', 'SPATIAL_CSW_ADMIN_USR', 'SPATIAL_WFS_ADMIN_USR'"
sql_text = """
select owner,table_name from dba_tab_privs where grantee='PEFDATA' and privilege='SELECT' and owner not in (%s) 
            union all
            select OWNER,table_name from dba_tables where owner='PEFDATA' and owner not in ({sys_user})
            union all
            select owner,table_name from role_tab_privs where role in (with data as (
            select t2.GRANTED_ROLE as role_cas,t1.GRANTED_ROLE from (select grantee,GRANTED_ROLE from dba_role_privs where grantee='PEFDATA' and GRANTED_ROLE not in ('CONNECT','RESOURCE','DBA')) t1 left join dba_role_privs t2 on t2.grantee =t1.GRANTED_ROLE where t1.grantee='PEFDATA')
            select data.role_cas from data union all select GRANTED_ROLE from data) and privilege='SELECT'
and owner not in ({sys_user}) AND owner='PUBLIC'
"""%sys_user
print(sql_text)