from decimal import Decimal
import hashlib
import json
import pymongo

from django.core.cache import cache
from django.db import connection
from movingup.data import db, mongo_fieldnames, sql_fieldnames
from movingup.occupations import OCCUPATIONS

DEFAULT_WEIGHTS = {
    'occupation_weight': 5,
    'childcare_weight': 1,
    'food_weight': 1,
    'transportation_weight': 1,
    'housing_weight': 3,
}

BASE_WEIGHT_DIVISOR = 3

SCORE_FIELDS = ('name','code','score', 'base_score', 'occupation_score',
    'childcare_score', 'food_score', 'gas_score', 'housing_score', 'salary')

def get_occupation(occs, occ_id, year='2010'):

    if occs is None:
        occs = OCCUPATIONS

    if occ_id not in occs:
        occ_id = '00-0000'

    available_years = [r['year'] for r in occs[occ_id]]

    if occ_id != '00-0000' and year not in available_years:
        occ_id = '00-0000'

    for record in occs[occ_id]:
        if record['year'] == year:
            record['id'] = occ_id
            return record

def get_occupation_name(occ_id):
    """ Given an occupation id, find the full name.
    """
    cat_id = occ_id.split('-')[0]
    category = OCCUPATIONS[int(cat_id)]
    for occ in category['occupations']:
        if occ['id'] == occ_id:
            return occ['name']

def get_value(d, key):
    if key == 'ffiec.avg':
        key = 'ffiec.diff'
    if '.' in key:
        (k, rest) = key.split('.', 1)
        try:
            val = get_value(d[k], rest)
        except KeyError:
            if k.startswith('rpp_local'):
                val = None
        return val
    else:
        return d[key]

def smartcmp(v1, v2, field=None):
    val = cmp(v1, v2)
    if field is not None and (field.startswith('naccrra') or field.startswith('rpp')):
        val *= -1
    return val

def cached_compare(occupation_id, location, compare_to=None, weights=None):

    key = "%s|%s" % (occupation_id, location['code'])

    if compare_to:
        key += "|%s" % compare_to['code']

    if weights:
        key += "|%s" % "|".join("%s:%s" % (k, weights[k]) for k in sorted(weights.iterkeys()))

    key = hashlib.md5(key).hexdigest()

    res = cache.get(key, None)
    if res is None:
        res = compare(occupation_id, location, compare_to, weights)
        cache.set(key, json.dumps(res))
    else:
        res = json.loads(res)

    return res

def compare(occupation_id, location, compare_to=None, weights=None):

    if weights is None:
        weights = DEFAULT_WEIGHTS.copy()

    occ = get_occupation(location['oes'], occupation_id, '2010')

    res = {
        'location': {
            'code': location['code'],
            'name': location['name'],
            'geo': location['geo'],
            'census': location.get('census', None),
        },
        'data': {
            'occupation': {
                'id': occupation_id,
                'value': occ['mean'],
                'is_average': occ['id'] == '00-0000',
            },
        },
        'scores': {},
        'weights': weights,
    }

    res['scores']['location'] = location_score(occupation_id, location['code'], weights)

    for fieldname in mongo_fieldnames():
        val = get_value(location, fieldname)
        if fieldname.startswith('rpp_state'):
            val = float(val)
        res['data'][fieldname.replace('.', '_')] = {
            'value': val,
        }

    if compare_to:

        res['compare_to'] = {
            'code': compare_to['code'],
            'name': compare_to['name'],
        }

        res['scores']['cmp'] = location_score(occupation_id, compare_to['code'], weights)

        occ = get_occupation(compare_to['oes'], occupation_id, '2010')

        res['data']['occupation']['compare_to'] = {
            'id': occ['id'],
            'value': occ['mean'],
            'cmp': cmp(res['data']['occupation']['value'], occ['mean']),
            'is_average': occ['id'] == '00-0000',
        }

        for fieldname in mongo_fieldnames():

            try:

                key = fieldname.replace('.', '_')
                val = get_value(compare_to, fieldname)

                if fieldname.startswith('rpp_state'):
                    val = float(val)

                res['data'][key]['compare_to'] = {
                    'value': val,
                    'cmp': smartcmp(res['data'][key]['value'], val, key),
                }

            except KeyError:
                res['data'][key]['compare_to'] = None

    else:
        res['scores']['cmp'] = {k: 0 for k in res['scores']['location'].iterkeys()}

    for k in res['scores']['cmp']:
        res['scores']['cmp'][k] = cmp(res['scores']['location'][k], res['scores']['cmp'][k])

    # national average

    fields = [f for f in sql_fieldnames()]

    stmt = """
        SELECT %s,
            lo.occupation AS occupation_id,
            lo.mean AS occupation_mean
        FROM locations l
        JOIN locations_occupations lo
            ON l.code = lo.code
        WHERE (lo.occupation = %%s OR lo.occupation = '00-0000')
            AND l.code = '00000'
            AND lo.year = '2010'
        ORDER BY lo.occupation DESC
    """ % ', '.join(fields)

    fields.extend(('occupation_id', 'occupation_mean'))

    cursor = connection.cursor()
    cursor.execute(stmt, (occupation_id,))

    doc = cursor.fetchone()
    row = dict(zip(fields, doc))
    cursor.close()

    def convert(v):
        if isinstance(v, Decimal):
            return float(v)
        else:
            return v

    national_occ = {}

    for k, v in row.iteritems():

        v = convert(v)

        if k in res['data']:

            if k.startswith('rpp_'):
                v = 100.0

            res['data'][k]['national'] = {
                'value': v,
                'cmp': smartcmp(res['data'][k]['value'], v, k),
            }

        elif k == 'occupation_mean':

            national_occ['value'] = v
            national_occ['cmp'] = cmp(res['data']['occupation']['value'], v)

        elif k == 'occupation_id':

            national_occ['id'] = v
            national_occ['is_average'] = v == '00-0000'


    res['data']['occupation']['national'] = national_occ

    return res

def location_score(occupation_id, code, weights=None):

    if weights is None:
        weights = DEFAULT_WEIGHTS

    cursor = connection.cursor()

    stmt = """
        SELECT (s.base_score / %s) +
            (s.occupation_score * %s) +
            (s.childcare_score * %s) +
            (s.food_score * %s) +
            (s.gas_score * %s) +
            (s.housing_score * %s) AS score,
            s.base_score, s.occupation_score, s.housing_score,
            s.childcare_score + s.food_score, s.gas_score + s.base_score
        FROM scores s
        JOIN locations l
            ON s.code = l.code
        WHERE (s.occupation = %s OR s.occupation = '00-0000') AND l.code = %s
        ORDER BY s.occupation DESC
    """

    params = (
        BASE_WEIGHT_DIVISOR,
        weights['occupation_weight'],
        weights['childcare_weight'],
        weights['food_weight'],
        weights['transportation_weight'],
        weights['housing_weight'],
        occupation_id,
        code,
    )

    resfields = ('score', 'base_score', 'occupation_score',
                 'housing_score', 'col_score', 'qol_score')

    cursor.execute(stmt, params)
    location = dict(zip(resfields, cursor.fetchone()))

    for k in location.iterkeys():
        if isinstance(location[k], Decimal):
            location[k] = float(location[k])

    location['score'] = \
        location['base_score'] + \
        location['occupation_score'] + \
        location['housing_score'] + \
        location['col_score'] + \
        location['qol_score']

    cursor.close()

    return location

def location_scores(occupation_id, current_location=None, weights=None):

    if weights is None:
        weights = DEFAULT_WEIGHTS

    cursor = connection.cursor()

    stmt = """
        SELECT l.name AS name, l.code AS code,
            (s.base_score / %s) +
                (s.occupation_score * %s) +
                (s.childcare_score * %s) +
                (s.food_score * %s) +
                (s.gas_score * %s) +
                (s.housing_score * %s) AS score,
            s.base_score, s.occupation_score, s.housing_score,
            s.childcare_score + s.food_score, s.gas_score + s.base_score
        FROM scores s JOIN locations l
            ON s.code = l.code
        WHERE s.occupation = %s AND l.code != %s
        ORDER BY score DESC
        LIMIT 30
    """

    params = (
        BASE_WEIGHT_DIVISOR,
        weights['occupation_weight'],
        weights['childcare_weight'],
        weights['food_weight'],
        weights['transportation_weight'],
        weights['housing_weight'],
        occupation_id,
        current_location or 'notreallyacode',
    )

    resfields = ('name', 'code', 'score', 'base_score', 'occupation_score',
                 'housing_score', 'col_score', 'qol_score')

    cursor.execute(stmt, params)
    locations = [dict(zip(resfields, row)) for row in cursor]
    cursor.close()

    for loc in locations:
        print loc['name'], loc, weights['occupation_weight']

    return locations

def all_locations(excluding=None):

    spec = {'good_to_go': True}

    if excluding:
        spec['code'] = {'$ne': excluding}

    fields = {
        '_id': 0,
        'code': 1,
        'name': 1,
        'primary_state': 1,
        'geo.centroid': 1,
    }

    locs = db.k2.locations.find(spec, fields).sort('name')

    return locs


def location_from_zip(zipcode):

    zip_doc = db.k2.zipcodes.find_one({'postal_code': zipcode})

    if zip_doc:

        spec = {'good_to_go': True, 'geo.centroid' : {'$near': zip_doc['latlng']}}
        fields = {'_id': 0, 'code': 1, 'name': 1, 'primary_state': 1, 'geo.centroid': 1}

        return db.k2.locations.find_one(spec, fields)

def nearby_locations(lat, lng, excluding=None, limit=10):
    """ Find locations close to the specified latitude and longitude.

        lat
            latitude
        lng
            longitude
        limit
            the number of results to return
    """

    lat = float(lat)
    lng = float(lng)

    spec = {'good_to_go': True, 'geo.centroid' : {'$near': [lat, lng]}}

    if excluding:
        spec['code'] = {'$ne': excluding}

    fields = {'_id': 0, 'code': 1, 'name': 1, 'primary_state': 1, 'geo.centroid': 1}

    locations = db.k2.locations.find(spec, fields).limit(limit)
    locations = [l for l in locations]

    return locations

def nearby_zipcodes(lat, lng):
    """ Find zipcodes that are near the provided latitude and longitude.
    """

    lat = float(lat)
    lng = float(lng)

    spec = {'latlng': {'$near': [lat, lng]}}
    fields = {'_id': 0, 'postal_code': 1, 'latlng': 1}

    zipcodes = db.k2.zipcodes.find(spec, fields).limit(10)
    zipcodes = [z for z in zipcodes]

    return zipcodes

