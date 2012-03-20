from saucebrush import emitters, filters, sources, run_recipe
import csv
import MySQLdb
import pymongo
import settings

class ValidMSAFilter(filters.ConditionalFilter):
    def test_record(self, record):
        return record['code']

class LocalLocationEmitter(emitters.Emitter):
    
    def __init__(self, **kwargs):
        super(LocalLocationEmitter, self).__init__(**kwargs)
        self._locations = pymongo.Connection()['k2']['locations']
    
    def emit_record(self, record):
        doc = self._locations.find_one({'code': record['code']})
        if doc:
            doc['rpp_local'] = {
                'overall': float(record['overall']),
                'goods': float(record['goods']),
                'services': float(record['services']),
            }
            self._locations.save(doc)

class StateLocationEmitter(emitters.Emitter):
    
    def __init__(self, **kwargs):
        super(StateLocationEmitter, self).__init__(**kwargs)
        self._locations = pymongo.Connection()['k2']['locations']
    
    def emit_record(self, record):
        if record['state_abbreviation']:
            data = {'rpp_state': {
                'rpp': record['rpp'],
                'rents': record['rents'],
                'apparel': record['apparel'],
                'education_goods': record['education_goods'],
                'education_services': record['education_services'],
                'food_goods': record['food_goods'],
                'food_services': record['food_services'],
                'housing_goods': record['housing_goods'],
                'housing_services': record['housing_services'],
                'medical_goods': record['medical_goods'],
                'medical_services': record['medical_services'],
                'other_goods': record['other_goods'],
                'other_services': record['other_services'],
                'recreation_goods': record['recreation_goods'],
                'recreation_services': record['recreation_services'],
                'transportation_goods': record['transportation_goods'],
                'transportation_services': record['transportation_services'],
            }}
            self._locations.update({'primary_state': record['state_abbreviation']}, {'$set': data}, multi=True)

def load_rpp():

    local = {}
    state = {}

    with open(settings.dataset_path('rpp', 'A5.coded.csv')) as infile:
        reader = csv.DictReader(infile)
        for record in reader:
            if record['code']:
                local[record['code']] = {
                    'overall': record['overall'],
                    'goods': record['goods'],
                    'services': record['services'],
                }
    
    with open(settings.dataset_path('rpp', 'A7.csv')) as infile:
        reader = csv.DictReader(infile)
        for record in reader:
            if record['state_abbreviation']:
                state[record['state_abbreviation']] = record.copy()
                del state[record['state_abbreviation']]['state']
                del state[record['state_abbreviation']]['state_abbreviation']
    
    locations = pymongo.Connection()['k2']['locations']

    db = MySQLdb.connect(
        user=settings.MYSQL_USER,
        passwd=settings.MYSQL_PASS,
        db=settings.MYSQL_DATABASE,
        host=settings.MYSQL_HOST,
    )

    cursor = db.cursor()

    for loc in locations.find({'good_to_go': True}):

        record = {}

        if loc['code'] in local:
            record.update(local[loc['code']])
        
        if loc['primary_state'] in state:
            record.update(state[loc['primary_state']])

        for key in record.iterkeys():
            record[key] = float(record[key])

        
        fields = ['code']
        values = [loc['code']]

        for key, value in record.iteritems():
            fields.append(key)
            values.append(value)

        stmt = "INSERT INTO rpp (%s) VALUES (%s)" % (",".join(fields), ",".join('%s' for v in values))

        print loc['code']

        cursor.execute(stmt, values)

    cursor.close()
    db.commit()
    


            



    # # load local data into mongo
    
    # path = settings.dataset_path('rpp', 'A5.coded.csv')

    # run_recipe(
    #     sources.CSVSource(open(path)),
    #     ValidMSAFilter(),
    #     LocalLocationEmitter(),
    #     #emitters.DebugEmitter(),
    # )

    # # load state data into mongo
    
    # path = settings.dataset_path('rpp', 'A7.csv')
    
    # run_recipe(
    #     sources.CSVSource(open(path)),
    #     StateLocationEmitter(),
    #     #emitters.DebugEmitter(),
    # )


if __name__ == '__main__':
    load_rpp()