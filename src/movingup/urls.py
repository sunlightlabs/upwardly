from django.conf.urls import patterns, include, url
from django.contrib import admin
from django.template import add_to_builtins
from django.views.generic.simple import direct_to_template

add_to_builtins('mediasync.templatetags.media')

admin.autodiscover()

urlpatterns = patterns('',
    url(r'^admin/', include(admin.site.urls)),
    url(r'^contact/$', 'movingup.views.contact', name='contact'),
    url(r'^debug/$', 'movingup.views.debug', name='debug'),
    url(r'^getstarted/$', 'movingup.views.get_started', name='get_started'),
    url(r'^locations/', include('movingup.locations.urls')),
    url(r'^methodology/$', direct_to_template, {'template': 'methodology.html'}, name='methodology'),
    url(r'^feedback/$', direct_to_template, {'template': 'feedback.html'}, name='feedback'),
    url(r'^occupations/$', 'movingup.views.occupations', name='occupation_categories'),
    url(r'^occupations/(?P<category_id>\d{2})/$', 'movingup.views.occupations', name='occupations'),
    url(r'^', include('mediasync.urls')),
    url(r'^$', 'movingup.views.home', name='home'),
)

"""
/
/getstarted/
/locations/
/locations/XXXXXX/
/locations/YYYYYY/?compareto=XXXXXX
/locations/XXXXXX/employment/
/locations/XXXXXX/housing/
/locations/XXXXXX/finances/
/locations/XXXXXX/qualityoflife/
/browse/
/about/
"""
