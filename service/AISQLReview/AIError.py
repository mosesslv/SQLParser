# -*- coding: UTF-8 -*-

"""
AI 异常及错误编码定义

字典key定义：
type: 错误类型, 指定在算法范围、数据范围

algorithm: 算法层处理问题
db: db层处理问题
data: 数据层问题

"""

AIError = {
    "AIErr_00001": {"type": "data", "description": "数据库引擎未知"},
    "AIErr_00002": {"type": "data", "description": "数据参数错误"},
    "AIErr_00003": {"type": "data", "description": "SCHEMA参数无效"},
    "AIErr_00004": {"type": "data", "description": "SQL文本无效"},
    "AIErr_00005": {"type": "data", "description": "租户参数无效"},
    "AIErr_00006": {"type": "algorithm", "description": "SQL语义分析异常"},
    "AIErr_00007": {"type": "data", "description": "SQL分析表名异常, 无法获取schema信息"},
    "AIErr_00008": {"type": "data", "description": "去O配合不合法"},

    "AIErr_Oracle_00001": {"type": "algorithm", "description": "SQL语法错误"},
    "AIErr_Oracle_00002": {"type": "algorithm", "description": "SQL信息缺失"},
    "AIErr_Oracle_00003": {"type": "algorithm", "description": "SQL信息冲突"},
    "AIErr_Oracle_00004": {"type": "db", "description": "DB初始化连接失败"},
    "AIErr_Oracle_00005": {"type": "algorithm", "description": "sqlparser处理异常"},
    "AIErr_Oracle_00006": {"type": "algorithm", "description": "sqlparser分析表名异常"},
    "AIErr_Oracle_00007": {"type": "data", "description": "表未上线，表结构信息不存在"},
    "AIErr_Oracle_00008": {"type": "data", "description": "直方信息获取异常"},
    "AIErr_Oracle_10000": {"type": "db", "description": "执行计划错误"},
    "AIErr_Oracle_10001": {"type": "db", "description": "缺失关键字导致执行计划错误"},    # ORA-00905
    "AIErr_Oracle_10002": {"type": "db", "description": "标示符无效导致执行计划错误"},  # ORA-00904
    "AIErr_Oracle_10003": {"type": "db", "description": "无效数据类型导致执行计划错误"},  # ORA-00902
    "AIErr_Oracle_10004": {"type": "db", "description": "列说明无效导致执行计划错误"},  # ORA-01747
    "AIErr_Oracle_10005": {"type": "db", "description": "参数无效导致执行计划错误"},  # ORA-00909
    "AIErr_Oracle_10006": {"type": "db", "description": "表或视图不存在导致执行计划错误"},  # ORA-00942
    "AIErr_Oracle_10007": {"type": "db", "description": "SQL表达式缺失导致执行计划错误"},  # ORA-00936
    "AIErr_Oracle_10008": {"type": "db", "description": "引号内的字符串没有正确结束导致执行计划错误"},  # ORA-01756
    "AIErr_Oracle_10009": {"type": "db", "description": "sql命令未正确结束导致执行计划错误"},  # ORA-00933
    "AIErr_Oracle_10010": {"type": "db", "description": "sql缺失右括号导致执行计划错误"},  # ORA-00907
    "AIErr_Oracle_10011": {"type": "db", "description": "标识符过长导致执行计划错误"},  # ORA-00972
    "AIErr_Oracle_10012": {"type": "db", "description": "无效表名导致执行计划错误"},  # ORA-00903
    "AIErr_Oracle_10013": {"type": "db", "description": "不是一对象或REF错误导致执行计划错误"},  # ORA-22806

    # "AIErr_Oracle_19999": {"type": "db", "description": "执行计划错误"},    # 保留给计划明细分析
    # "AIErr_Oracle_20000": {"type": "algorithm", "description": "执行计划错误"}, - 30000  #infomation handler
    "AIErr_Oracle_20000": {"type": "algorithm", "description": "表信息收集不完整，AI无法做合理判断"},
    "AIErr_Oracle_20001": {"type": "algorithm", "description": "缺失执行计划，AI无法做合理判断"},
    "AIErr_Oracle_20002": {"type": "algorithm", "description": "LUParser 解析错误，AI无法做合理判断"},
    "AIErr_Oracle_20003": {"type": "algorithm", "description": "表信息收集不完整，AI无法做合理判断"},
    "AIErr_Oracle_20004": {"type": "algorithm", "description": "该sql不是oracle"},
    "AIErr_Oracle_20005": {"type": "algorithm", "description": "统计信息不准确，AI无法作出合理判断"},

    "AIErr_Oracle_99997": {"type": "algorithm", "description": "AI判定处理异常"},
    "AIErr_Oracle_99998": {"type": "algorithm", "description": "AI推荐处理异常"},
    "AIErr_Oracle_99999": {"type": "algorithm", "description": "AI处理异常"},

    "AIErr_Mysql_00001": {"type": "algorithm", "description": "SQL语法错误"},
    "AIErr_Mysql_00002": {"type": "algorithm", "description": "SQL信息缺失"},
    "AIErr_Mysql_00003": {"type": "algorithm", "description": "SQL信息冲突"},
    "AIErr_Mysql_00004": {"type": "db", "description": "DB初始化连接失败"},
    "AIErr_Mysql_00005": {"type": "algorithm", "description": "sqlparser处理异常"},
    "AIErr_Mysql_00006": {"type": "algorithm", "description": "sqlparser分析表名异常"},
    "AIErr_Mysql_00007": {"type": "data", "description": "表未上线，表结构信息不存在"},
    "AIErr_Mysql_10000": {"type": "db", "description": "执行计划错误"},
    "AIErr_Mysql_10001": {"type": "db", "description": "语法错误导致执行计划错误"},
    "AIErr_Mysql_10002": {"type": "db", "description": "未知字段导致执行计划错误"},
    "AIErr_Mysql_10003": {"type": "db", "description": "表不存在导致执行计划错误"},
    "AIErr_Mysql_10004": {"type": "db", "description": "FUNCTION不存在导致执行计划错误"},
    "AIErr_Mysql_10005": {"type": "db", "description": "执行计划错误,Every derived table must have its own alias"},  # Every derived table must have its own alias


    # "AIErr_Oracle_19999": {"type": "db", "description": "执行计划错误"},    # 保留给计划明细分析
    # "AIErr_Oracle_20000": {"type": "algorithm", "description": "执行计划错误"}, - 30000  #infomation handler
    "AIErr_Mysql_20000": {"type": "algorithm", "description": "表信息收集不完整，AI无法做合理判断"},
    "AIErr_Mysql_20001": {"type": "algorithm", "description": "缺失执行计划，AI无法做合理判断"},
    "AIErr_Mysql_20002": {"type": "algorithm", "description": "LUParser 解析错误，AI无法做合理判断"},
    "AIErr_Mysql_20003": {"type": "algorithm", "description": "表信息收集不完整，AI无法做合理判断"},
    "AIErr_Mysql_20004": {"type": "algorithm", "description": "该sql不是mysql"},
    "AIErr_Mysql_20005": {"type": "algorithm", "description": "无法获取表内统计数据，请确认表格中是否存有数据，目前AI无法作出合理判断"},
    "AIErr_Mysql_20006": {"type": "algorithm", "description": "使用关键词作为表的别名，AI无法作出合理判断"},

    "AIErr_Mysql_99997": {"type": "algorithm", "description": "AI判定处理异常"},
    "AIErr_Mysql_99998": {"type": "algorithm", "description": "AI推荐处理异常"},
    "AIErr_Mysql_99999": {"type": "algorithm", "description": "AI判定处理异常"},

}


def get_error_type_description(ai_err_code) -> (str, str):
    """
    根据 code 返回错误描述
    :param ai_err_code:
    :return:
    """
    try:
        desc_dict = AIError.get(ai_err_code)
        if desc_dict is None:
            type = "unknown"
            desc = "{0} 未定义".format(ai_err_code)
        else:
            type = desc_dict.get("type")
            desc = desc_dict.get("description")

            if type is None:
                type = "unknown"

            if desc is None:
                desc = "{0} 未定义".format(ai_err_code)
    except Exception as ex:
        type = "unknown"
        desc = "{0} 未定义".format(ai_err_code)
    return type, desc


if __name__ == "__main__":
    msg = get_error_type_description("AIErr_Oracle_00003")
    print(msg)
