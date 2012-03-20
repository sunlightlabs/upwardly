from django.conf.urls.defaults import patterns, include, url

urlpatterns = patterns('movingup.locations.views',
    url(r'^$', 'index', name='index'),
    url(r'^browse/$', 'browse', name='browse'),
    url(r'^filter/$', 'filter', name='filter'),
    url(r'^nearby/$', 'nearby', name='nearby'),
    url(r'^zipcodes/$', 'zipcodes', name='zipcodes'),
    url(r'^(?P<msa>\d{5})/$', 'overview', name='overview'),
    url(r'^(?P<msa>\d{5})/employment/$', 'employment', name='employment'),
    url(r'^(?P<msa>\d{5})/finances/$', 'finances', name='finances'),
    url(r'^(?P<msa>\d{5})/housing/$', 'housing', name='housing'),
    url(r'^(?P<msa>\d{5})/lifestyle/$', 'lifestyle', name='lifestyle'),
)