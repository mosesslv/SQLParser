# -*- coding:utf-8 -*-

"""
grpc channel
实现LUFAX 业务
"""

# create protobuf
# /wls/virtualenv/ai_python3_virtualenv/bin/python -m grpc_tools.protoc -I /DATA1/projects/python/sqlreview/service/grpc_channel/ --python_out=/DATA1/projects/python/sqlreview/service/grpc_channel/ --grpc_python_out=/DATA1/projects/python/sqlreview/service/grpc_channel/ /DATA1/projects/python/sqlreview/service/grpc_channel/oracle_sql_predict.proto
# /wls/virtualenv/ai_python3_virtualenv/bin/python -m grpc_tools.protoc -I /DATA1/projects/python/sqlreview/service/grpc_channel/ --python_out=/DATA1/projects/python/sqlreview/service/grpc_channel/ --grpc_python_out=/DATA1/projects/python/sqlreview/service/grpc_channel/ /DATA1/projects/python/sqlreview/service/grpc_channel/data_service.proto