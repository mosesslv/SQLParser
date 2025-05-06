# -*- coding:utf-8 -*-
# CREATED BY: bohuai jiang 
# CREATED ON: 2020/1/3 上午9:35
# LAST MODIFIED ON:
# AIM: this is a demo

from service.predict_table_strategy_oracle.InfoHandler import Handler_pingan
from service.predict_table_strategy_oracle.InfoExtraction import Extract
from service.predict_table_strategy_oracle.StrategyBuilder import StrategyBuilder

class OracleTabStrategy:

    def __init__(self):
        pass

    def handle_struct_data(self):
        pass

    def collect_sql(self):
        collector = Extract()

        for each_sql in share_pool:
            oracleSQLStruct = get_oracle_sql_struct(each_sql)

            #先经过 handle:
            # alert: SQLStruct 必须包含:
            #                           sql text
            #                           oracle_conn
            #                           tab_info
            handle = Handler_pingan(oracleSQLStruct)
            # handler 会将信息打包,
            # 下一步将handle类传给collector
            collector.run(handle)


        # builder 是个独立类不需要传惨
        builder = StrategyBuilder()
        # run 输出str, 可以打印到终端或者dump成文本
        print(builder.run())