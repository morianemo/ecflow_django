from django.conf.urls import include
from django.urls import path, re_path
from django.views.generic import ListView, DetailView
from django.contrib import admin, admindocs
from django.contrib.staticfiles.urls import staticfiles_urlpatterns
from django.views.generic.base import RedirectView

# from django.urls import re_path as url
from . import views

admin.autodiscover()
urlpatterns = [ # 'fdi.views', # Examples:
    path('', views.home),
    re_path(r'api/ecflow/.*$', views.process),
    re_path(r'api/sms/.*$', views.process),
    path('slide2', views.slide2),
    path('slide3', views.slide3),
    path('triggers', views.triggers),
    path('json', views.tojson),
    path('edge', views.edge),
    path('vars', views.vars),
    path('admin/doc/', include('django.contrib.admindocs.urls')),
    path('admin/', admin.site.urls),
]

urlpatterns += staticfiles_urlpatterns()

SITE_ID = 1

# if fdi.settings.DEBUG:
#     import debug_toolbar
#     urlpatterns += patterns('',
#         url(r'^__debug__/', include(debug_toolbar.urls)), )

REST_FRAMEWORK = { 'DEFAULT_PERMISSION_CLASSES': ['rest_framework.permissions.DjangoModelPermissionsOrAnonReadOnly' ]    }
# LOGIN_REDIRECT_URL = "sst"

