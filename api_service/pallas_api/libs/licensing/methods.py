# -*- coding: utf-8 -*-
"""
Created on Thu Jan 24 08:06:39 2019

@author: liujun693
"""

import platform
import uuid
import sys
from .internal import HelperMethods
from .models import *
import json


class Key:
    """
    License key related methods. More docs: http://172.28.10.7:5000/docs/api/v3/Key.
    """

    @staticmethod
    def activate(token, product_id, machine_code, user,
                 hwid):

        """
        Calls the Activate method in Web API 3 and returns a tuple containing
        (LicenseKey, Message). If an error occurs, LicenseKey will be None. If
        everything went well, no message will be returned.

        More docs: http://172.28.10.7:5000/docs/api/v3/Activate
        """

        response = Response("", "", 0, "")

        try:
            response = Response.from_string(HelperMethods.send_request("activate", {"token": token, \
                                                                                    "app_id": product_id, \
                                                                                    "machine": machine_code, \
                                                                                    "user": user, \
                                                                                    "hwid": hwid}))
        except Exception as e:
            import traceback
            print(traceback.format_exc())
            print(e)
            return (None, "Could not contact the server.")

        # return (response.result, LicenseKey.from_response(response))
        return (response.result, response)

    @staticmethod
    def check(token, product_id, machine_code, user, \
              hwid):

        """
        Calls the Activate method in Web API 3 and returns a tuple containing
        (LicenseKey, Message). If an error occurs, LicenseKey will be None. If
        everything went well, no message will be returned.

        More docs: http://172.28.10.7:5000/docs/api/v3/Activate
        """

        response = Response("", "", 0, "")

        try:
            response = Response.from_string(HelperMethods.send_request("check", {"token": token, \
                                                                                 "app_id": product_id, \
                                                                                 "machine": machine_code, \
                                                                                 "user": user, \
                                                                                 "hwid": hwid}))
        except Exception:
            return (None, "Could not contact the server.")

        return (response.result, LicenseKey.from_response(response))


class Helpers:

    @staticmethod
    def GetMachineCode():

        """
        Get a unique identifier for this device.
        """

        if "windows" in platform.platform().lower():
            return HelperMethods.get_SHA256(
                HelperMethods.start_process(["cmd.exe", "/C", "wmic", "csproduct", "get", "uuid"]))
        elif "mac" in platform.platform().lower() or "darwin" in platform.platform().lower():
            res = HelperMethods.start_process(["system_profiler", "SPHardwareDataType"])
            return HelperMethods.get_SHA256(res[res.index("UUID"):].strip())
        elif "linux" in platform.platform().lower():
            return HelperMethods.get_SHA256(HelperMethods.compute_machine_code())
        else:
            return HelperMethods.get_SHA256(HelperMethods.compute_machine_code())

    @staticmethod
    def IsOnRightMachine(license_key, is_floating_license=False, allow_overdraft=False):

        """
        Check if the device is registered with the license key.
        """

        current_mid = Helpers.GetMachineCode()
        if license_key.hwid == None:
            print("**"*10)
            print("current: %s" % current_mid)
            print("license: %s" % license_key.hwid)
            return False

        for act_machine in license_key.hwid:
            if current_mid == act_machine:
                return True
        print("current: %s" % current_mid)
        print("license: %s" % license_key.hwid)
        return False
