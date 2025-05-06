# -*- coding:utf-8 -*-

import hashlib
import math
import struct


class OraUtils:
    """
    ORACLE OFFLINE UTILS

    SQL_TEXT to SQL_ID AND HASH :
    Tanel published a great post a while ago talking about Oracle’s sql_id and hash values in Oracle 10g+.
    I wanted to be able to compute sql_id and hash values directly from SQL statements for our Hedgehog product.
    I did a few tests and could not match the MD5 value generated from the SQL statement to the MD5 value
    Oracle is calculating in X$KGLOB.KGLNAHSV.
    After a short discussion with Tanel, it turned out that Oracle is appending a NULL (‘\0’) value to
    the statement and then calculates the MD5.
    http://www.slaviks-blog.com/2010/03/30/oracle-sql_id-and-hash-value/
    """
    def __init__(self):
        pass

    # noinspection PyMethodMayBeStatic
    def smtp_2_hash(self, sql_text):
        """
        get sql_text hash
        :param sql_text:
        :return:
        """
        if sql_text is None or len(sql_text.strip()) <= 0:
            return ""

        stmt = sql_text
        d = hashlib.md5(stmt + '\x00').digest()
        hash_val = struct.unpack('IIII', d)[3]

        h = ""
        for i in struct.unpack('IIII', d):
            h += hex(i)[2:]

        return hash_val

    # noinspection PyMethodMayBeStatic
    def sqlid_2_hash(self, sqlid):
        """
        get hash value from sql_id
        :param sqlid:
        :return: hash
        """
        sum = 0
        i = 1
        alphabet = '0123456789abcdfghjkmnpqrstuvwxyz'
        for ch in sqlid:
            sum += alphabet.index(ch) * (32 ** (len(sqlid) - i))
            i += 1
        return sum % (2 ** 32)

    # noinspection PyMethodMayBeStatic
    def stmt_2_sqlid(self, sql_text):
        """
        get sql_id from sql_text
        :param sql_text:
        :return: sql_id
        """
        if sql_text is None or len(sql_text.strip()) <= 0:
            return ""

        h = hashlib.md5(str(sql_text) + '\x00').digest()
        (d1, d2, msb, lsb) = struct.unpack('IIII', h)
        sqln = msb * (2 ** 32) + lsb
        stop = math.log(sqln, math.e) / math.log(32, math.e) + 1
        sqlid = ''
        alphabet = '0123456789abcdfghjkmnpqrstuvwxyz'
        for i in range(0, int(stop)):
            sqlid = alphabet[(sqln / (32 ** i)) % 32] + sqlid
        return sqlid

    # noinspection PyMethodMayBeStatic
    def stmt_2_hash(self, sql_text):
        """
        get hash from sql_text
        :param sql_text:
        :return:
        """
        if sql_text is None or len(sql_text.strip()) <= 0:
            return ""
        return struct.unpack('IIII', hashlib.md5(str(sql_text) + '\x00').digest())[3]

if __name__ == "__main__":
    ora_utils = OraUtils()
    print(ora_utils.smtp_2_hash("select 'Slavik' from dual"))
    print(ora_utils.stmt_2_sqlid("select 'Slavik' from dual"))
    print(ora_utils.sqlid_2_hash("29schpgjyfxux"))
    print(ora_utils.smtp_2_hash("select 'Slavik' from dual"))
