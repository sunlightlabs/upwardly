from saucebrush import run_recipe
from saucebrush.emitters import DebugEmitter, Emitter, CSVEmitter
from saucebrush.filters import FieldMerger, FieldRemover, Filter
from saucebrush.sources import SqliteSource
import json
import os
import requests
import settings
import sqlite3

class GeocoderFilter(Filter):

    ENDPOINT = "http://where.yahooapis.com/geocode"

    def __init__(self, appid, field):
        super(GeocoderFilter, self).__init__()
        self.appid = appid
        self.field = field

    def geocode(self, address):

        params = {
            'q': address,
            'appid': self.appid,
            'flags': 'CJ',
        }

        resp = requests.get(self.ENDPOINT, params=params)
        data = json.loads(resp.content)

        try:

            result = data['ResultSet']['Results'][0]
            return (result['latitude'], result['longitude'])

        except KeyError:
            pass
        except IndexError:
            pass

        return (None, None)

    def process_record(self, record):

        ll = self.geocode(record['address'])
        
        record['latitude'] = ll[0]
        record['longitude'] = ll[1]

        return record

class LocationUpdateEmitter(Emitter):
    
    def __init__(self, db_path):
        super(LocationUpdateEmitter, self).__init__()
        self._conn = sqlite3.connect(db_path)
        
    def emit_record(self, record):
        stmt = """UPDATE locations SET latitude = ?, longitude = ? WHERE city = ? AND state = ? AND zipcode = ?"""
        params = (
            record['latitude'],
            record['longitude'],
            record['city'],
            record['state'],
            record['zipcode'],
        )
        self._conn.execute(stmt, params)
        self._conn.commit()
    
    def done(self):
        self._conn.close()

db_path = settings.dataset_path('default', filename='load_nces.db')
csv_path = settings.dataset_path('nces', filename='school_locations.csv')

run_recipe(
    SqliteSource(db_path, """SELECT * FROM locations WHERE latitude = '' and longitude = ''"""),
    FieldMerger({'address': ('city','state','zipcode')}, lambda c, s, z: "%s, %s %s" % (c, s, z), keep_fields=True),
    GeocoderFilter('Kv3.btLV34EuebZGMzi1KaqI_BOPhPjx7FtbvED.umr8DGUq0NysoGN0XIIIDRU-', 'address'),
    FieldRemover('address'),
    CSVEmitter(open(csv_path, 'w'), fieldnames=('city','state','zipcode','latitude','longitude')),
    DebugEmitter(),
)

