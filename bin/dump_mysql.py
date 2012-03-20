from movingup.data import DATA_FIELDS, sql_fieldnames
from saucebrush import emitters, filters, run_recipe
from MySQLdb import cursors
import MySQLdb
import pymongo

class K2LocationSource(object):

	def __iter__(self):

		conn = pymongo.Connection()

		spec = {
			'oes': {'$exists': True},
			'ffiec': {'$exists': True},
			'nces': {'$exists': True},
			#'rpp_local': {'$exists': True}, # don't require RPP local since DC doesn't have it
			'rpp_state': {'$exists': True},
			'naccrra': {'$exists': True}
		}

		locations = conn.k2.locations.find(spec)

		for location in locations:
			yield location

class DataFilter(filters.Filter):

	def nested_get(self, record, keys):
		value = record
		for key in keys:
			value = value[key]
		return value

	def flatten_fields(self, record, fields, stack=None):

		if stack is None:
			stack = []

		for key, value in fields.iteritems():
			if isinstance(value, dict):
				self.flatten_fields(record, value, stack + [key])
			else:
				for field in value:
					keys = stack + [key, field]
					field_key = "_".join(keys)
					record[field_key] = self.nested_get(record, keys)

	def process_record(self, record):

		if 'rpp_local' not in record:
			record['rpp_local'] = {
				'goods': None,
				'overall': None,
				'services': None,
			}

		self.flatten_fields(record, DATA_FIELDS)

		record['geo_centroid_lat'] = record['geo']['centroid'][0]
		record['geo_centroid_lng'] = record['geo']['centroid'][1]

		def decurrencify(s):
			return s.strip().replace('$', '').replace(',', '') + '.0'
		
		for key, value in record.iteritems():
			if key.startswith('naccrra_'):
				record[key] = decurrencify(value)

		return record

class MySQLLocationEmitter(emitters.Emitter):
	
	def __init__(self, user, passwd, db, host='localhost', port=3306, table="locations"):
		super(MySQLLocationEmitter, self).__init__()
		self._conn = MySQLdb.connect(user=user, passwd=passwd, db=db, host=host, port=port)
		self._cursor = self._conn.cursor()
		self._table = table

		stmt = """DELETE FROM %s""" % table
		self._cursor.execute(stmt)
	
	def emit_record(self, record):
		
		fields = []
		values = []

		for key, value in record.iteritems():
			fields.append(key)
			values.append(value)
		
		fields = ", ".join(fields)
		placeholders = ", ".join("%s" for i in xrange(len(values)))

		stmt = """INSERT INTO %s (%s) VALUES (%s)""" % (self._table, fields, placeholders)
		self._cursor.execute(stmt, values)
	
	def done(self):
		self._cursor.close()
		self._conn.commit()
		self._conn.close()

class MySQLOccupationEmitter(emitters.Emitter):

	def __init__(self, user, passwd, db, host='localhost', port=3306, table="locations_occupations"):
		super(MySQLOccupationEmitter, self).__init__()
		self._conn = MySQLdb.connect(user=user, passwd=passwd, db=db, host=host, port=port)
		self._cursor = self._conn.cursor()
		self._table = table
	
	def emit_record(self, record):
		
		for occ, records in record['oes'].iteritems():

			for rec in records:

				values = (
					record['code'],
					occ,
					int(rec['year']),
					int(rec['mean']),
					int(rec['median']),
					1 if rec['is_major'] else 0,
					1 if rec['is_total'] else 0,
				)

				placeholders = ", ".join("%s" for i in xrange(len(values)))

				stmt = """INSERT INTO %s (code, occupation, year, mean, median, is_major, is_total) VALUES (%s)""" % (self._table, placeholders)
				self._cursor.execute(stmt, values)

	
	def done(self):
		self._cursor.close()
		self._conn.commit()
		self._conn.close()


def load_locations():
	run_recipe(
		K2LocationSource(),
		DataFilter(),
		filters.FieldRemover(('_id','ffiec','geo','oes','naccrra','nces','rpp_local','rpp_state')),
		MySQLLocationEmitter('root', '', 'k2'),
		#emitters.DebugEmitter(),
		error_stream = emitters.DebugEmitter(),
	)

def load_occupations():
	run_recipe(
		K2LocationSource(),
		MySQLOccupationEmitter('root', '', 'k2'),
		#emitters.DebugEmitter(),
		error_stream = emitters.DebugEmitter(),
	)

def score():

	conn = MySQLdb.connect(user='root', passwd='', db='k2', host='localhost', port=3308)
	cursor = conn.cursor()

	stats = {}
	
	for field in sql_fieldnames():

		stmt = """SELECT AVG(%s), STDDEV_POP(%s) FROM locations""" % (field, field)
		cursor.execute(stmt)

		for row in cursor:
			stats[field] = row

	print stats

	cursor.close()

	cursor = conn.cursor(cursors.DictCursor)

	stmt = """SELECT * FROM locations"""
	cursor.execute(stmt)

	for row in cursor:
		
		print row['name']

		total_score = 0

		for field in sql_fieldnames():
			(avg, stddev) = stats[field]
			diff = float(row[field] - avg)
			score = int(diff / stddev)

			if field.startswith('naccrra_') or field.startswith('rpp_'):
				score *= -1

			total_score += score

			print ' ', field, score, avg, row[field]
		
		print '    ', total_score

if __name__ == '__main__':

	load_locations()
	load_occupations()
	#score()
