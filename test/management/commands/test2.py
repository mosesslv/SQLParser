# -*- coding: UTF-8 -*-

from django.core.management.base import BaseCommand


def test_ai_sqlreview_handle(seq):
    from service.GitSQLReview.sqlreview_handle import SQLReviewHandle, SQLReviewStruct
    from api_service.pallas_api.models import AiSrReviewDetail

    sql_detail = AiSrReviewDetail.objects.get(sequence=seq)

    sql_struct = SQLReviewStruct()
    sql_struct.sql_text = sql_detail.sql_new_text
    sql_struct.sqlmap_filename = sql_detail.sqlmap_files
    sql_struct.namespace = sql_detail.namespace
    sql_struct.sqlmap_id = sql_detail.sqlmap_id
    sql_struct.review_request_id = sql_detail.review_request_id
    sql_struct.sql_sequence = sql_detail.sequence
    sql_handle = SQLReviewHandle(sql_struct)
    sql_handle.get_ai_predict()

    # result = sql_handle.write_database()

    print("hello")


class Command(BaseCommand):
    def handle(self, *args, **options):
        # import pdb;pdb.set_trace()
        # rowid = int(args[0])
        test_ai_sqlreview_handle("9ab6d2175fc340b9a1097880e3f3a5c1")
        print("finished")
