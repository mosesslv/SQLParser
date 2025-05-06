"""
title = ''
author = 'lvao513'
mtime = '2020/5/26'
"""
from typing import Dict
from test.test_Utility.Utility_Data_get_from_sql import GetSQL

sql = """SELECT sql_text FROM dbcm.ai_sr_sql_detail limit 10"""
sql_getter = GetSQL(sql_text=sql)
data = sql_getter.get_data()
print(data)

def get_columns_distinct_ratio(self,tab_name:str) -> Dict[str,float]:
    """
    calculate
    the
    column
    value
    ration
    get
    olumns_distinct_ratio
    :param
    tab_name:
    :return: Dict -> {value1: ratio1, value2: ratio2, ..., value_n, ration}
    """
    from service.predict_sql_review_oracle.Utility.TableInfoAnalysis import TabelInfoAnaylsis
    tb_info = TabelInfoAnaylsis()
    pass

