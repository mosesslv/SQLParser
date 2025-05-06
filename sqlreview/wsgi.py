"""
WSGI config for sqlreview project.

It exposes the WSGI callable as a module-level variable named ``application``.

For more information on this file, see
https://docs.djangoproject.com/en/2.2/howto/deployment/wsgi/
"""

import os

from django.core.wsgi import get_wsgi_application
from api_service.pallas_api.libs.licensing.verify_licens import verify_license
from sqlreview.settings import LICENSE_KEY

verify_license(LICENSE_KEY)

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'sqlreview.settings')

application = get_wsgi_application()
