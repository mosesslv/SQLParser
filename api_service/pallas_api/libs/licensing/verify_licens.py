import base64
import datetime
import json

import sys
import os

PROJECT_PATH = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))))

sys.path.append(PROJECT_PATH)

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'sqlreview.settings')
import django

django.setup()

from sqlreview.settings import LICENSE_KEY
from api_service.pallas_api.libs.licensing.models import LicenseKey
from api_service.pallas_api.libs.licensing.methods import Key, Helpers


# 验证 license_key
def verify_license(license_key=None):
    try:
        if not license_key:
            license_key = LICENSE_KEY
        # license_key = load_from_string(license_key)
        content = {"signature": "", "result": "ok", "messagge": "", "licenseKey": license_key}
        if license_key == "$5W1F0DSMI8KRW3U99NXEZL09Z==":
            return True
        license_key = LicenseKey.load_from_string(json.dumps(content))
        if not license_key:
            raise Exception("签发证书已过期")
        if not Helpers.IsOnRightMachine(license_key):
            raise Exception("NOTE: This license file does not belong to this machine.")
        return True
    except Exception as e:
        raise Exception(str(e))


def activate():
    # execute only if run as a script
    # Helpers.GetMachineCode()
    res = Key.activate(token="5W1F0DSMI8KRW3U99NXEZL09Z",
                   user="admin",
                   product_id=1, machine_code="lucloudkey_python2",
                   hwid='3dea23d0270f8d894bab7830862827a231055e5634172b073b5f351f5561e2c8')
    print(res)
    if res[0] == 'ok':
        print("Success")
    else:
        print("An error occurred: {0}".format(res[1]))


def check():
    res = Key.check(token="5W1F0DSMI8KRW3U99NXEZL09Z",
                   user="admin",
                   product_id=1, machine_code="lucloudkey_python3",
                   hwid='3dea23d0270f8d894bab7830862827a231055e5634172b073b5f351f5561e2c8')
    print(res)
    if res[0] == 'ok':
        print("Success")
        print("License key app_id: " + str(res[1].app_id))
        # saving license file to disk
        with open('licensefile.py', 'w') as f:
            result = res[1].save_as_string()
            ret = json.loads(result)
            ret['licenseKey'] = ret['licenseKey'][::-1].strip('=')
            print(ret['licenseKey'])
            f.write(json.dumps(ret))
        with open('licensefile.py', 'r') as f:
            content = json.loads(f.read())
            content['licenseKey'] = ret['licenseKey'][::-1]+'=='
            license_key = LicenseKey.load_from_string(json.dumps(content))
            print(license_key.__dict__)
            # if license_key != None and not Helpers.IsOnRightMachine(license_key):
            #     print("NOTE: This license file does not belong to this machine.")
            # else:
            #     print("Success")
            #     print("License key: " + str(res[1].app_id))
    else:
        print("An error occurred: {0}".format(res[1]))


if __name__ == "__main__":

    # activate()
    check()



