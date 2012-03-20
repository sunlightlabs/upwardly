from saucebrush import sources, emitters, filters, run_recipe
import pymongo

FIELD_NAMES = ('name','code','overall','goods','services')

class MSAFilter(filters.Filter):
    
    def __init__(self):
        super(MSAFilter, self).__init__()
        self._locations = pymongo.Connection()['k2']['locations']
    
    def process_record(self, record):
        doc = self._locations.find_one({'name': record['name'].strip()})
        if doc:
            record['code'] = doc['code']
        return record

def code_a5():
    
    run_recipe(
        sources.CSVSource(open('/Users/Jeremy/Downloads/A5.csv')),
        filters.FieldAdder('code', None),
        MSAFilter(),
        emitters.CSVEmitter(open('/Users/Jeremy/Downloads/A5.coded.csv', 'w'), fieldnames=FIELD_NAMES),
    )

def code_a7():
    pass

if __name__ == '__main__':
    pass