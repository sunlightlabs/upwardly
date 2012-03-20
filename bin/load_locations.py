from saucebrush import run_recipe
from saucebrush.emitters import SqliteEmitter, DebugEmitter, MongoDBEmitter
from saucebrush.filters import Filter, FieldRemover, ConditionalFilter
from saucebrush.sources import CSVSource
import csv
import os
import re
import settings

HEADERS = ('code','name')

def msa_iter():
	msa_re = re.compile(r"^(\d{5})\s{19}(.*) (Metro|Micro)politan Statistical Area$")
	txt_path = settings.dataset_path('locations', filename='List1.txt')
	for line in (l.strip() for l in open(txt_path)):
	    m = msa_re.match(line)
	    if m is not None:
	        yield dict(zip(HEADERS, m.groups()[:2])) 

class GeoJSONTestFilter(Filter):
    
    geo_path = os.path.join(settings.dataset_path('default'), 'geojson', '%s.json')
    
    def process_record(self, record):
        path = self.geo_path % record['code']
        if not os.path.exists(path):
            print "!!!! not found [%s] %s" % (record['code'], record['name'])
        return record

class CensusTestFilter(Filter):
    
    def __init__(self):
        super(CensusTestFilter, self).__init__()
        self._msas = []
        csv_path = settings.dataset_path('default', filename='census.csv')
        for record in csv.DictReader(open(csv_path)):
            self._msas.append(record['msa'])
        self._msas = set(self._msas)

    def process_record(self, record):
        if record['code'] not in self._msas:
            print "!!! no census data [%s] %s" % (record['code'], record['name'])
        return record

class GeoFilter(Filter):
    def process_record(self, record):
        ll = [float(l) for l in reversed(record['centroid'].split(','))]
        record['geo'] = {'centroid': ll}
        del record['centroid']
        return record

class MSAFilter(ConditionalFilter):
    def test_record(self, record):
        return len(record['code']) == 5

def load_locations():
    
    csv_path = settings.datasource_path('locations', filename='locations.csv')
    
    # load locations into mongodb
    run_recipe(
        CSVSource(open(csv_path)),
        FieldRemover('points'),
        MSAFilter(),
        GeoJSONTestFilter(),
        CensusTestFilter(),
        GeoFilter(),
        DebugEmitter(),
        #MongoDBEmitter(settings.MONGO_DATABASE, 'locations'),
    )

if __name__ == '__main__':

	print "Loading areas..."
	load_locations()

	print "Done!"