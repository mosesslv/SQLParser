# -*- coding:utf-8 -*-
"""
陆金所生产环境发送短信
"""
import subprocess
import requests
import logging
logger = logging.getLogger("")


def lu_prd_send_sms(mobile_list, sms_content):
    """
    send sms
    :param mobile_list:
    :param sms_content:
    :return:
    """
    url = "http://alarm.op.lufax.tool/api_v1/message/sms"
    # data = {
    #     "source": "moth",                                                           #发送源系统名
    #     "id": "sms-test",                                                             #发送源系统下的子类型
    #     "content": "sms test",                                                        #短信内容
    #     "to": "xueguanlu002",                                              #发送人列表, 支持um帐号和普通手机号格式
    # }

    data = {
        "source": "dpaa",
        "id": "dpaa publish",
        "content": sms_content,
        "to": ",".join(mobile_list)
    }

    try:
        r = requests.post(url, data=data)
        ret = r.json()
        logger.info(ret)

    except Exception as e:
        logger.error("call post sms api exception: {0}".format(e))


def lu_prd_send_sms_from_prd(mobile_list, sms_content):
    """
    send sms
    :param mobile_list:
    :param sms_content:
    :return:
    """
    # curl -i --url http://sms-mon.lufax.gw/sms-gw/service/sms/send -d "dto={\"mobileNos\":\"13361813096\",\"smsContent\":\"[ORA-STB-AUTOSWITCH][$send_text]\",\"channelName\":\"etonenet_03\"}"

    for mobile in mobile_list:
        sms_command = "curl -i --url http://sms-mon.lufax.gw/sms-gw/service/sms/send -d \"dto={{\\\"mobileNos\\\":\\\"{0}\\\",\\\"smsContent\\\":\\\"{1}\\\",\\\"channelName\\\":\\\"etonenet_03\\\"}}\"".format(mobile, sms_content)
        try:
            process = subprocess.Popen(
                sms_command,
                shell=True
            )
            stdoutdata, stderrdata = process.communicate()
            returncode = process.returncode

        except OSError as e:
            # 系统异常
            error_info = "[ERROR {0}] {1}".format(e.errno, e.strerror)
        except KeyboardInterrupt:
            # 键盘中断
            error_info = "exited after ctrl-c"
            try:
                process.terminate()
            except OSError:
               pass
            process.communicate()
        except Exception as e:
            error_info = "{0}".format(e)


# demo
if __name__ == "__main__":
    mobile_list = ['18016392206']
    lu_prd_send_sms(mobile_list, "test")
