import itertools

from pymongo import Connection
from saucebrush import emitters, filters, sources, stats, run_recipe, Recipe
import os
import re
import settings

PRIMARY_STATE_RE = re.compile('^(.*), ([A-Z]{2})$')

class LocationSource(object):
    
    fields = (
        'code', 'name', 'primary_state', 'occupation',
        'ffiec_low', 'ffiec_high', 'ffiec_avg',
        'nces_schools',
        'oes_median', 'oes_mean',
    )
    
    def __init__(self):
        self._locations = Connection()[settings.MONGO_DATABASE]['locations']
    
    def __iter__(self):
        
        for location in self._locations.find({}):
            
            if 'code' not in location:
                continue
            
            record = dict((key, None) for key in self.fields)
            record.update({
                'code': location['code'],
                'name': location['name'],
                'occupation': '00-0000',
            })
            
            primary_state = location.get('primary_state', None)
            if primary_state is None:
                m = PRIMARY_STATE_RE.match(location['name'])
                if m is not None:
                    primary_state = m.groups()[1]
            record['primary_state'] = primary_state
            
            if 'ffiec' in location:
                record['ffiec_low'] = location['ffiec']['low']
                record['ffiec_high'] = location['ffiec']['high']
                record['ffiec_avg'] = location['ffiec']['avg']
            
            if 'nces' in location:
                record['nces_schools'] = location['nces']['schools']
            
            yield record
            
            if 'oes' in location:
                
                for occupation_id, oes in location['oes'].iteritems():
                
                    record['occupation'] = occupation_id
                    record['oes_median'] = oes['median']
                    record['oes_mean'] = oes['mean']
                
                    yield record
        


if __name__ == '__main__':
    
    csv_path = os.path.join(settings.DATA_DIR, 'k2.csv')
    db_path = os.path.join(settings.DATA_DIR, 'k2.db')
    
    # if os.path.exists(db_path):
    #     os.unlink(db_path)
    # 
    # run_recipe(
    #     LocationSource(),
    #     #emitters.CSVEmitter(open(csv_path, 'w'), fieldnames=LocationSource.fields),
    #     emitters.SqliteEmitter(db_path, 'locations', fieldnames=LocationSource.fields),
    #     #emitters.MongoDBEmitter(settings.MONGO_DATABASE, 'movingup', port=settings.MONGO_PORT)
    #     #emitters.DebugEmitter(),
    # )
    
    def to_float(s):
        if s is not None:
            try:
                return float(s)
            except ValueError:
                pass
    
    def fieldnames_iter(fieldnames):
        yield 'occupation'
        for f in fieldnames:
            yield "%s_stddev" % f
            yield "%s_mean" % f
            
    STATS_FIELDS = ('ffiec_low','ffiec_high','ffiec_avg','nces_schools','oes_median','oes_mean')
    
    class StatsGenerator(filters.Filter):
        def process_record(self, record):
            
            occ = record['occupation']
            
            stats_filters = {}
            
            for fieldname in STATS_FIELDS:
                stats_filters[fieldname] = stats.StandardDeviation(fieldname)
            
            run_recipe(
                sources.SqliteSource(db_path, """SELECT * FROM locations WHERE occupation = ?""", (occ,)),
                filters.FieldModifier(STATS_FIELDS, to_float),
                Recipe(*stats_filters.values()),
                error_stream = emitters.DebugEmitter(),
            )
            
            for fieldname, stats_filter in stats_filters.iteritems():
                record['%s_stddev' % fieldname] = stats_filter.value()[0]
                record['%s_mean' % fieldname] = stats_filter.average()
            
            return record
    
    stats_path = os.path.join(settings.DATA_DIR, 'k2-stats.csv')
    
    run_recipe(
        sources.SqliteSource(db_path, """SELECT DISTINCT occupation FROM locations"""),
        StatsGenerator(),
        emitters.CSVEmitter(
            open(stats_path, 'w'),
            fieldnames=[f for f in fieldnames_iter(STATS_FIELDS)],
        ),
        emitters.DebugEmitter(),
        error_stream = emitters.DebugEmitter(),
    )