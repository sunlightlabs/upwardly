from saucebrush import emitters, filters, sources, run_recipe
import os
import pymongo
import settings

STATES = {
    'AK': 'Alaska',
    'AL': 'Alabama',
    'AR': 'Arkansas',
    'AS': 'American Samoa',
    'AZ': 'Arizona',
    'CA': 'California',
    'CO': 'Colorado',
    'CT': 'Connecticut',
    'DC': 'District of Columbia',
    'DE': 'Delaware',
    'FL': 'Florida',
    'GA': 'Georgia',
    'GU': 'Guam',
    'HI': 'Hawaii',
    'IA': 'Iowa',
    'ID': 'Idaho',
    'IL': 'Illinois',
    'IN': 'Indiana',
    'KS': 'Kansas',
    'KY': 'Kentucky',
    'LA': 'Louisiana',
    'MA': 'Massachusetts',
    'MD': 'Maryland',
    'ME': 'Maine',
    'MI': 'Michigan',
    'MN': 'Minnesota',
    'MO': 'Missouri',
    'MP': 'Northern Mariana Islands',
    'MS': 'Mississippi',
    'MT': 'Montana',
    'NA': 'National',
    'NC': 'North Carolina',
    'ND': 'North Dakota',
    'NE': 'Nebraska',
    'NH': 'New Hampshire',
    'NJ': 'New Jersey',
    'NM': 'New Mexico',
    'NV': 'Nevada',
    'NY': 'New York',
    'OH': 'Ohio',
    'OK': 'Oklahoma',
    'OR': 'Oregon',
    'PA': 'Pennsylvania',
    'PR': 'Puerto Rico',
    'RI': 'Rhode Island',
    'SC': 'South Carolina',
    'SD': 'South Dakota',
    'TN': 'Tennessee',
    'TX': 'Texas',
    'UT': 'Utah',
    'VA': 'Virginia',
    'VI': 'Virgin Islands',
    'VT': 'Vermont',
    'WA': 'Washington',
    'WI': 'Wisconsin',
    'WV': 'West Virginia',
    'WY': 'Wyoming'
}
STATES = {value: key for key, value in STATES.iteritems()}

class MongoNACCRRAEmitter(emitters.Emitter):
    
    def __init__(self, **kwargs):
        super(MongoNACCRRAEmitter, self).__init__(**kwargs)
        self._locations = pymongo.Connection()[settings.MONGO_DATABASE]['locations']
    
    def emit_record(self, record):
        
        state_abbr = STATES.get(record['state'], None)
        
        if state_abbr is None:
            self.reject_record(record, ValueError('invalid state name: %s' % record['state']))
        
        record_copy = record.copy()
        del record_copy['state']
        
        data = {'naccrra': record_copy}
        
        self._locations.update({'primary_state': state_abbr}, {'$set': data}, multi=True)

def load_naccrra():
    
    csv_path = os.path.join(settings.dataset_path('default'), 'childcarecosts.csv')
    
    run_recipe(
        sources.CSVSource(open(csv_path)),
        filters.FieldRenamer({
            'state': 'State',
            'family_infant': 'Family-Infant',
            'family_4': 'Family-4-Year-Old',
            'family_school': 'Family-School-Age',
            'center_infant': 'Center-Infant',
            'center_4': 'Center-4-Year-Old',
            'center_school': 'Center-School-Age',
        }),
        MongoNACCRRAEmitter(),
        emitters.CountEmitter(),
        #emitters.DebugEmitter(),
        error_stream = emitters.DebugEmitter(),
    )

if __name__ == '__main__':
    
    load_naccrra()