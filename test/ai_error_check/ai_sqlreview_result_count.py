import datetime
import json
from common.MysqlHandle import *
from django.http import HttpResponse
import smtplib
from sqlreview import settings
from email.message import EmailMessage as Mail


def ai_sqlreview_result_count():
    """

    :param start_time: xxxx-xx-xx
    :param end_time: xxxx-xx-xx
    :return:send email
    """
    try:
        start_time_d = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        end_time_d = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y-%m-%d %H:%M:%S')
        # start_time_d = str(start_time) + " 00:00:00"
        # end_time_d = str(end_time) + " 00:00:00"
        logging.info("即将获取从{0}到{1}的ai统计个数".format(start_time_d, end_time_d))
        try:
            dbcm = MysqlHandle(settings.DATABASES['default']['USER'], settings.DATABASES['default']['PASSWORD'],
                               settings.DATABASES['default']['NAME'], settings.DATABASES['default']['HOST'],
                               settings.DATABASES['default']['PORT'])
        except Exception as e:
            msg = "dbcm MysqlHandle exception:{0}".format(e)
            logging.info(msg)
            # return HttpResponse(json.dumps({"message": msg}))

        all_count_sql_text = """
        select d.app_name,count(b.id) from sr_review_request a,sr_review_detail b,sr_review_detail_extend c,
        sr_app_repository d where a.id=b.review_request_id and b.id=c.detail_id and a.app_id=d.id and
        a.created_at > '{0}' and a.created_at < '{1}' and c.message not like '%_Rendezvous%' group by d.app_name;
        """.format(start_time_d, end_time_d)

        pass_count_sql_text = """
                select count(c.ai_result),d.app_name from sr_review_request a,sr_review_detail b,sr_review_detail_extend c,
                sr_app_repository d where a.id=b.review_request_id and b.id=c.detail_id and a.app_id=d.id and
                 c.ai_result='PASS' and a.created_at > '{0}' and a.created_at < '{1}' and c.message not like '%_Rendezvous%' group by d.app_name;
                """.format(start_time_d, end_time_d)

        nopass_count_sql_text = """
                select count(c.ai_result),d.app_name from sr_review_request a,sr_review_detail b,sr_review_detail_extend c,
                sr_app_repository d where a.id=b.review_request_id and b.id=c.detail_id and a.app_id=d.id and
                 c.ai_result='NOPASS' and a.created_at > '{0}' and a.created_at < '{1}' and c.message not like '%_Rendezvous%' group by d.app_name;
                """.format(start_time_d, end_time_d)

        invalid_count_sql_text = """
                select count(c.ai_result),d.app_name from sr_review_request a,sr_review_detail b,sr_review_detail_extend c,
                sr_app_repository d where a.id=b.review_request_id and b.id=c.detail_id and a.app_id=d.id and
                 c.ai_result='INVALID' and a.created_at > '{0}' and a.created_at < '{1}' and c.message not like '%_Rendezvous%' group by d.app_name;
                """.format(start_time_d, end_time_d)

        ai_error_info_sql_text = """
                select message,count(*) cnt from sr_review_detail_extend where updated_at > '{0}' and updated_at < '{1}' and
                 message not like '%_Rendezvous%' and ai_result<>'PASS' and ai_result<>'NOPASS' group by message order by cnt desc;
        """.format(start_time_d, end_time_d)

        all_count_sql_text_list = dbcm.mysql_execute_query_get_all_data(all_count_sql_text).data
        pass_count_sql_text_list = dbcm.mysql_execute_query_get_all_data(pass_count_sql_text).data
        nopass_count_sql_text_list = dbcm.mysql_execute_query_get_all_data(nopass_count_sql_text).data
        invalid_count_sql_text_list = dbcm.mysql_execute_query_get_all_data(invalid_count_sql_text).data
        ai_error_info_sql_text_list = dbcm.mysql_execute_query_get_all_data(ai_error_info_sql_text).data

        if len(all_count_sql_text_list) > 0 and len(pass_count_sql_text_list) > 0 and len(nopass_count_sql_text_list) > 0 \
                and len(invalid_count_sql_text_list) > 0 and len(ai_error_info_sql_text_list) > 0:
            print('数据都已经获取到')
        else:
            print('数据某部分未获取到')
        # delete_ai_sqlreview_result_all_data = dbcm.mysql_execute_dml_sql("truncate table ai_sqlreview_result;").result

        error_info_head_list = ['AI错误类型', '个数']
        error_info_headwidth = ['800', '100']
        error_info_table_content = []
        for extend in ai_error_info_sql_text_list:
            content = []
            content.append(extend[0])
            content.append(extend[1])
            error_info_table_content.append(content)

        if len(all_count_sql_text_list) > 0:
            mhm = MailHtmlModule()
            review_count_headwidth = ['400', '200', '200', '200', '200']
            mail = Mail()
            # send_list = ['xuxuewu319@lu.com', 'ML_10381@pingan.com.cn']
            send_list = ['LVAO513@pingan.com.cn']
            review_count_table_content = []
            for all_ai in all_count_sql_text_list:
                try:
                    a_content = []
                    all_count_app_name = all_ai[0]
                    all_review_count = all_ai[1]
                    a_content.append(all_count_app_name)
                    a_content.append(all_review_count)
                    for pass_ai in pass_count_sql_text_list:
                        if str(all_count_app_name) == str(pass_ai[1]):
                            a_content.append(str(pass_ai[0]))
                            break
                    if len(a_content) == 2:
                        a_content.append('0')

                    for nopass_ai in nopass_count_sql_text_list:
                        if str(all_count_app_name) == str(nopass_ai[1]):
                            a_content.append(str(nopass_ai[0]))
                            break
                    if len(a_content) == 3:
                        a_content.append('0')

                    for invalid_ai in invalid_count_sql_text_list:
                        if str(all_count_app_name) == str(invalid_ai[1]):
                            a_content.append(str(invalid_ai[0]))
                            break
                    if len(a_content) == 4:
                        a_content.append('0')
                    review_count_table_content.append(a_content)
                except Exception as e:
                    logger.error(e)

            all_count_sum = 0
            pass_count_sum = 0
            nopass_count_sum = 0
            invalid_count_sum = 0
            for tab_contet in review_count_table_content:
                all_count_sum += int(tab_contet[1])
                pass_count_sum += int(tab_contet[2])
                nopass_count_sum += int(tab_contet[3])
                invalid_count_sum += int(tab_contet[4])


            review_count_head_list = ['应用名', 'review sql数(总:{0})'.format(all_count_sum), 'AI review通过数(总:{0})'.format(pass_count_sum),
                         'AI review不通过数(总:{0})'.format(nopass_count_sum),
                         'AI review异常数(总:{0})'.format(invalid_count_sum)]

            html_data_list = []
            if len(review_count_table_content) > 0:
                review_count_html_data = {'title': 'AIreview统计', 'head': review_count_head_list, 'headwidth': review_count_headwidth,
                                          'table': review_count_table_content}
                html_data_list.append(review_count_html_data)

            if len(error_info_table_content) > 0:
                error_info_html_data = {'title': 'AIreview错误类型统计', 'head': error_info_head_list, 'headwidth': error_info_headwidth,
                             'table': error_info_table_content}
                html_data_list.append(error_info_html_data)

            if len(review_count_table_content) > 0 or len(error_info_table_content) > 0:
                mhm.table_module_2(html_data_list)
                mail.send_mail('ai_sqlreview@lujs.cn', send_list, 'AI_sqlreview统计', mhm.html_file_path)
                os.remove(mhm.html_file_path)

        logging.info("已经成功获取从{0}到{1}的ai统计个数".format(start_time_d, end_time_d))
        return HttpResponse(json.dumps({'message': 'success'}))
    except Exception as e:
        print(e)
        logging.info(e)
        return HttpResponse(json.dumps({"message": "{0}".format(e)}))

