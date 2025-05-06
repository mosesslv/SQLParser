# -*- coding:utf-8 -*-
# CREATED BY: bohuai jiang 
# CREATED ON: 2020/2/13 下午2:33
# LAST MODIFIED ON:
# AIM:

from common.OracleHandle import OracleHandle

class OracleSharePoolHandle(OracleHandle):

    def __init__(self, username:str, passwd:str, instance_name:str, host_exc:str, host_actual:str, port:str, snapshot:str):
        super().__init__(username, passwd, instance_name, host_exc, int(port))
        self.snapshot = snapshot
        self.host_actual = host_actual

    def get_instance_url(self) -> str:
        instance_url = "{0}:{1}/{2} {3}/{4}".format(self.host_actual,
                                                    self.get_port(),
                                                    self.get_instance_name(),
                                                    self.get_username(),
                                                    self.get_passwd())
        return instance_url
