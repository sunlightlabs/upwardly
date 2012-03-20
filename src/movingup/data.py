from django.conf import settings
import pymongo

DATA_FIELDS = {
    'ffiec': [
        'avg',
        'high',
        'low',
    ],
    'naccrra': [
        'center_4',
        'center_infant',
        #'center_school',
        'family_4',
        'family_infant',
        #'family_school',
    ],
    # 'rpp_local': [
    #     'goods',
    #     'overall',
    #     'services',
    # ],
    'rpp_state': [
        'apparel',
        'education_goods',
        'education_services',
        'food_goods',
        'food_services',
        'housing_goods',
        'housing_services',
        'medical_goods',
        'medical_services',
        'other_goods',
        'other_services',
        'recreation_goods',
        'recreation_services',
        'rents',
        'rpp',
        'transportation_goods',
        'transportation_services',
    ]
}

def sql_fieldnames():
    for namespace, names in DATA_FIELDS.iteritems():
        for name in names:
            yield "%s_%s" % (namespace, name)

def mongo_fieldnames():
    for namespace, names in DATA_FIELDS.iteritems():
        for name in names:
            yield "%s.%s" % (namespace, name)

def map_fieldnames():
    for namespace, names in DATA_FIELDS.iteritems():
        for name in names:
            yield ("%s.%s" % (namespace, name), "%s_%s" % (namespace, name))

class LazyDatabase(object):

    def __init__(self):
        self._dbs = {}

    def __getattr__(self, attr):

        if attr not in self._dbs:

            host = getattr(settings, 'MONGO_HOST', 'localhost')
            port = getattr(settings, 'MONGO_PORT', 27017)
            name = getattr(settings, 'MONGO_DATABASE', None)

            conn = pymongo.Connection(host, port)
            self._dbs[attr] = conn[name]

        return self._dbs.get(attr, None)

db = LazyDatabase()