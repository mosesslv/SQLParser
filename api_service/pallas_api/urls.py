from django.conf import settings
from django.conf.urls import url
from django.urls import include
from rest_framework.documentation import include_docs_urls

from api_service.pallas_api.views.account import LoginView, AiSrUserView, UserInfoView
from api_service.pallas_api.views.common_view import TestConnectivityView, RepoView, GitSqlReviewView, TagReviewView, \
    SQLReviewView
from api_service.pallas_api.views.views import AiSrSqlDetailView, AiSrTaskView, AiSrTaskSqlView, ModelCommonQueryView, \
    PallasIndexDataView, AiSrProfileManageView, AiSrTenantSchemaView, AiSrSchemaTableView, AiSrAppRepositoryView, \
    AiSrTaskValidView
from api_service.pallas_api.views.openapi import GitSqlReviewAPIView, CheckTagReviewResultView, GetTableNameBySQLView, \
    GetSqlListByXMLView

# from rest_framework import routers
#
# router = routers.DefaultRouter()
# router.register(r'servers', ServerViewSet, base_name='servers')


urlpatterns = [
    # url(r'^', include(router.urls)),
    url(r'^task$', AiSrSqlDetailView.as_view(), name='ai_sr_sql_detail'),
    url(r'^api/ci/(?P<table_name>[^/]+)$', ModelCommonQueryView.as_view(), name='ai_sr_sql_detail'),
    # views.views
    url(r'^api/v1/ai_sr_sql_detail$', AiSrSqlDetailView.as_view(), name='ai_sr_sql_detail'),
    url(r'^api/v1/ai_sr_task$', AiSrTaskView.as_view(), name='ai_sr_task'),
    url(r'^api/v1/ai_sr_task/valid$', AiSrTaskValidView.as_view(), name='ai_sr_task'),
    url(r'^api/v1/ai_sr_task_sql$', AiSrTaskSqlView.as_view(), name='ai_sr_task_sql'),
    url(r'^api/v1/ai_sr_profile_manage$', AiSrProfileManageView.as_view(), name='ai_sr_profile_manage'),
    url(r'^api/v1/ai_sr_tenant_schema$', AiSrTenantSchemaView.as_view(), name='ai_sr_tenant_schema'),
    url(r'^api/v1/ai_sr_schema_table$', AiSrSchemaTableView.as_view(), name='ai_sr_schema_table'),
    url(r'^api/v1/ai_sr_app_repository$', AiSrAppRepositoryView.as_view(), name='ai_sr_app_repository'),
    url(r'^api/v1/index_data$', PallasIndexDataView.as_view(), name='ai_sr_task_sql'),

    # views.common_view
    url(r'^api/v1/test_conn$', TestConnectivityView.as_view()),
    url(r'^api/v1/repo$', RepoView.as_view()),
    url(r'^api/v1/exec_task_by_git$', GitSqlReviewView.as_view()),
    url(r'^api/v1/review_result$', TagReviewView.as_view()),
    url(r'^common/review$', SQLReviewView.as_view()),

    # views.account
    url(r'^api/v1/login$', LoginView.as_view()),
    url(r'^api/v1/ai_sr_user$', AiSrUserView.as_view()),
    url(r'^api/v1/user_info', UserInfoView.as_view()),

    # views.openapi
    url(r'^openapi/sqlreview_by_git$', GitSqlReviewAPIView.as_view()),
    url(r'^openapi/check_review$', CheckTagReviewResultView.as_view()),
    url(r'^openapi/get_table_name_by_sql$', GetTableNameBySQLView.as_view()),
    url(r'^openapi/get_sql_list_by_xml$', GetSqlListByXMLView.as_view()),
    url(r'^docs/', include_docs_urls(title='documentation', authentication_classes=[], schema_url=None if settings.API_DOCS else None)),
]
