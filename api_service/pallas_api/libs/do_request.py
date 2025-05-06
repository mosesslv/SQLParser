import time
import requests
import logging
import json

logger = logging.getLogger(__name__)

RETRY_STATUS = [502, 503, 504]
RETRY_INTERVER = 3
RETRY_NUM = 5


# --------------------------------------------------------------------------
##
# @synopsis  get
#
# @param url
# @param token
#
# @returns   status_code and self-defined json dict
# ----------------------------------------------------------------------------
def get(url, token=None):

    headers = {'Content-Type': 'application/json'}
    if token:
        headers['token'] = token

    r = None
    num = 0
    while(num < RETRY_NUM):
        r = requests.get(url, headers=headers)
        if r.status_code not in RETRY_STATUS:
            break
        num = num + 1
        time.sleep(RETRY_INTERVER)

    ret_dict = {}
    try:
        ret_dict = r.json()
    except Exception as e:
        ret_dict['message'] = 'response.json() error. Status code: %s,  Reason: %s, Content: %s' % (str(r.status_code), r.reason, r.text)
        # for wrap_get to get message field
        logger.exception('exception capture: ' + str(e))
    return r.status_code, ret_dict


# --------------------------------------------------------------------------
##
# @synopsis  post
#
# @param url
# @param data  THis is a DICT
# @param token
#
# @returns
# ----------------------------------------------------------------------------
def post(url, data, token=None):

    headers = {'Content-Type': 'application/json'}
    if token:
        headers['token'] = token

    r = None
    num = 0
    while(num < RETRY_NUM):
        r = requests.post(url, headers=headers, json=data)
        if r.status_code not in RETRY_STATUS:
            break
        num = num + 1
        time.sleep(RETRY_INTERVER)


    logger.debug("do_request post r.content %s", r.content)
    ret_dict = {}
    try:
        ret_dict = r.json()
    except Exception as e:
        # for wrap_get to get message field
        ret_dict['message'] = 'response.json() error. Status code: %s,  Reason: %s, Content: %s' % (str(r.status_code), r.reason, r.text)
        logger.exception('exception capture: ' + str(e))
    return r.status_code, ret_dict
    # try:
    #     ret_json = r.json()
    #     ret_code = r.status_code
    #     return ret_code, ret_json
    # except Exception as e:
    #     logger.exception('do_request exec error' + ERROR_MSG_DELIMITER +
    #                      str(e))
    #     logger.error("r.content %s %s", r, r.content)
    #     return r.status_code, r.json()


def post_pure(url, data, token=None):

    headers = {'Content-Type': 'application/json'}
    if token:
        headers['token'] = token
    r = requests.post(url, headers=headers, json=data)
    logger.info("do_request post pure r.content %s r.status_code %s" %(r.content, r.status_code))
    ret_dict = {}
    try:
        ret_dict = r.json()
    except Exception as e:
        # for wrap_get to get message field
        ret_dict['message'] = 'response.json() error. Status code: %s,  Reason: %s, Content: %s' % (str(r.status_code), r.reason, r.text)
        logger.exception('exception capture: ' + str(e))
    return r.status_code, ret_dict

def put(url, data, token=None):

    headers = {'Content-Type': 'application/json'}
    if token:
        headers['token'] = token

    r = None
    num = 0
    while(num < RETRY_NUM):
        r = requests.put(url, headers=headers, json=data)
        if r.status_code not in RETRY_STATUS:
            break
        num = num + 1
        time.sleep(RETRY_INTERVER)

    ret_dict = {}
    try:
        ret_dict = r.json()
    except Exception as e:
        # for wrap_get to get message field
        ret_dict['message'] = 'response.json() error. Status code: %s,  Reason: %s, Content: %s' % (str(r.status_code), r.reason, r.text)
        logger.exception('exception capture: ' + str(e))
    return r.status_code, ret_dict


def delete(url, data=None, token=None):

    headers = {'Content-Type': 'application/json'}
    if token:
        headers['token'] = token

    r = None
    num = 0
    while(num < RETRY_NUM):
        r = requests.delete(url, headers=headers, json=data)
        if r.status_code not in RETRY_STATUS:
            break
        num = num + 1
        time.sleep(RETRY_INTERVER)

    ret_dict = {}
    try:
        ret_dict = r.json()
    except Exception as e:
        # for wrap_get to get message field
        ret_dict['message'] = 'response.json() error. Status code: %s,  Reason: %s, Content: %s' % (str(r.status_code), r.reason, r.text)
        logger.exception('exception capture: ' + str(e))
    return r.status_code, ret_dict
