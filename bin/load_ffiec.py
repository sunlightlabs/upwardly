from pymongo import Connection
from saucebrush import run_recipe
from saucebrush.emitters import SqliteEmitter, DebugEmitter, Emitter
from saucebrush.filters import FieldMerger, FieldModifier, ConditionalFilter
import os

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
FFIEC_ROOT = os.path.join(PROJECT_ROOT, 'data', 'ffiec')

DB = os.path.join(PROJECT_ROOT, 'data', 'load.db')
HEADERS = ('msa_code','name','low','high','avg')

class MongoEmitter(Emitter):
    
    def __init__(self):
        super(MongoEmitter, self).__init__()
        self._mongo = Connection()['k2']['locations']
    
    def emit_record(self, record):
        doc = self._mongo.find_one({'code': record['msa_code']})
        if doc:
            doc['ffiec'] = {
                'low': record['low'],
                'high': record['high'],
                'avg': record['avg'],
            }
            self._mongo.save(doc)
        else:
            print "[%s] %s not found" % (record['msa_code'], record['name'])
    
    def done(self):
        pass

class GenericMSAFilter(ConditionalFilter):
    def test_record(self, record):
        return record['msa_code'] != '99999'

def ffiec_iter():

	size = 4
	lines = []
	infile = open(os.path.join(FFIEC_ROOT, 'msa11inc.txt'))

	for index, line in enumerate(infile):
		line = line.strip()
		if line:
			lines.append(line)
			if len(lines) % size == 0:
				yield dict(zip(HEADERS, lines))
				lines = []
	
	infile.close()

def load_ffiec():

	run_recipe(
		ffiec_iter(),
		FieldModifier(('low','high'), float),
		FieldMerger({'avg': ('low','high')}, lambda x, y: (x + y) / 2, keep_fields=True),
		#SqliteEmitter(DB, 'ffiec_incomes', fieldnames=HEADERS),
		GenericMSAFilter(),
		MongoEmitter(),
		#DebugEmitter(),
	)

if __name__ == '__main__':

	raw_input("FFIEC data must be manually fixed before loading. Press enter once this has been completed.")

	print "Loading FFIEC..."
	load_ffiec()

	print "Done!"
