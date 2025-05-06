"""sqlreview URL Configuration

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/2.2/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""
from django.contrib import admin
from django.urls import path, re_path
from django.conf.urls import url, include
from api_service.httprequest.http_factory_service import http_service_factory

urlpatterns = [
    path('admin/', admin.site.urls),

    # http request - use factory
    url(r"^service/http_api$", http_service_factory, name="http_service_factory"),

    # pallas_api - use factory
    url(r"^pallas/", include("api_service.pallas_api.urls")),
    # re_path(r'^search/', include('haystack.urls')),
]
