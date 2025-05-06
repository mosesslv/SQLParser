from pprint import pprint

import requests

headers = {
    'AUTHORIZATION': '4775b36cfc156ef849ee6c9939da4082',
    'PASSPORT': '1'
}
# data = {
#     'size':160,
#     'volume_id':'Ebs-C60OlQEV2B',
#
#     'server_name': 'ECB-LIX700215'
# }
res = requests.get(
    'http://127.0.0.1:8000/pallas/api/v1/ai_sr_task_sql?limit=15&offset=0&task_id=5114aac429d24c79be9d91a1d8bef97e&ai_result=NOPASS&create_at__gte=&create_at__lte=&sql_sequence=',
    headers=headers)
# print(res.json())
print(res.status_code)
pprint(res.content)
pprint(res.json())
