from pymongo import Connection
from saucebrush import run_recipe
from saucebrush.emitters import DebugEmitter, Emitter
from saucebrush.filters import FieldKeeper, FieldMerger, FieldModifier
from saucebrush.sources import CSVSource
from saucebrush.stats import Histogram
import csv
import settings

class MongoZipEmitter(Emitter):
    
    def __init__(self):
        super(MongoZipEmitter, self).__init__()
        self._zipcodes = Connection()[settings.MONGO_DATABASE]['zipcodes']
        self._zipcodes.drop()
    
    def emit_record(self, record):
        self._zipcodes.insert(record)

def load_zipcodes():
    
    headers = (
        'country_code','postal_code','name',
        'state_name','state_code',
        'county_name','county_code',
        'community_name','community_code',
        'latitude','longitude','accuracy'
    )
    
    state_histogram = Histogram('state_code')
    state_histogram.label_length = 2
    
    csv_path = settings.dataset_path('default', filename='zipcodes.txt')
    
    run_recipe(
        CSVSource(open(csv_path), delimiter="\t", fieldnames=headers),
        FieldKeeper(('postal_code','name','state_code','latitude','longitude')),
        FieldModifier(('latitude','longitude'), float),
        FieldMerger({'latlng': ('latitude', 'longitude')}, lambda lat, lng: (lat, lng)),
        #MongoZipEmitter(),
        #DebugEmitter(),
        state_histogram,
    )
    
    return str(state_histogram)

if __name__ == '__main__':
    print load_zipcodes()