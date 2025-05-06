import json

import requests
try:
        data = {"tag": "xxxx",
                "app_name": "xxxx"}
        headers = {"Content-Type": "application/json"}
        raise Exception(data)
        ret = requests.post("http://127.0.0.1:8000/pallas/openapi/sqlreview_by_git", data=json.dumps(data), headers=headers)

        print(ret.status_code)
        print(ret.content)
except Exception as e:
        e = str(e)
        print(json.loads(e))
        print(type(e))

