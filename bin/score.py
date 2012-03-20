from movingup.data import sql_fieldnames
from saucebrush import emitters, filters, run_recipe
from MySQLdb import cursors
import MySQLdb
import settings

class ScoreSource(object):

	def __init__(self, **kwargs):
		super(ScoreSource, self).__init__(**kwargs)
		self._conn = MySQLdb.connect(
			user=settings.MYSQL_USER,
			passwd=settings.MYSQL_PASS,
			db=settings.MYSQL_DATABASE,
			host=settings.MYSQL_HOST,
			port=settings.MYSQL_PORT,
		)

		cursor = self._conn.cursor()

		self._stats = {}

		for field in sql_fieldnames():

			stmt = """SELECT AVG(%s), STDDEV_POP(%s) FROM locations""" % (field, field)
			cursor.execute(stmt)

			for row in cursor:
				self._stats[field] = row

		#cursor.execute("""DELETE FROM scores""")

		cursor.close()

	def __iter__(self):

		cursor = self._conn.cursor(cursors.DictCursor)

		stmt = """SELECT * FROM locations"""
		cursor.execute(stmt)

		for row in cursor:

			record = {
				'code': row['code'],
			}

			for field in sql_fieldnames():

				value = row[field]

				if value is None:

					score = 0
				else:

					(avg, stddev) = self._stats[field]
					diff = float(value - avg)
					score = int(diff / stddev)

					if field.startswith('naccrra_') or field.startswith('rpp_'):
						score *= -1

				record[field] = score

			yield record

		cursor.close()

class OccupationScoreFilter(filters.YieldFilter):

	def __init__(self, **kwargs):

		super(OccupationScoreFilter, self).__init__(**kwargs)

		self._conn = MySQLdb.connect(
			user=settings.MYSQL_USER,
			passwd=settings.MYSQL_PASS,
			db=settings.MYSQL_DATABASE,
			host=settings.MYSQL_HOST,
			port=settings.MYSQL_PORT,
		)

		self._stats = {}

		cursor = self._conn.cursor()

		stmt = """SELECT occupation, AVG(mean) AS mean_avg, STDDEV_POP(mean) AS mean_stddev, AVG(median) AS median_avg, STDDEV_POP(median) AS median_stddev FROM locations_occupations WHERE year = 2010 GROUP BY occupation"""
		cursor.execute(stmt)

		for row in cursor:
			self._stats[row[0]] = {
				'mean_avg': row[1],
				'mean_stddev': row[2],
				'median_avg': row[3],
				'median_stddev': row[4],
			}

		cursor.close()

	def process_record(self, record):

		cursor = self._conn.cursor(cursors.DictCursor)

		stmt = """SELECT * FROM locations_occupations WHERE year = 2010 AND code = %s"""
		cursor.execute(stmt, (record['code'],))

		for row in cursor:

			stats = self._stats[row['occupation']]

			avg = stats['mean_avg']
			stddev = stats['mean_stddev']

			if stddev == 0:
				score = 0
			else:
				diff = float(row['mean'] - avg)
				score = int(diff / stddev)

			score += row['is_major'] * 2

			sub_record = record.copy()

			sub_record['occupation'] = row['occupation']
			sub_record['salary_mean'] = row['mean']
			sub_record['bls_mean'] = score

			yield sub_record

		cursor.close()

class MySQLEmitter(emitters.Emitter):

	def __init__(self, table, **kwargs):
		super(MySQLEmitter, self).__init__(**kwargs)
		self._table = table
		self._conn = MySQLdb.connect(
			user=settings.MYSQL_USER,
			passwd=settings.MYSQL_PASS,
			db=settings.MYSQL_DATABASE,
			host=settings.MYSQL_HOST,
			port=settings.MYSQL_PORT,
		)
		self._cursor = self._conn.cursor(cursors.DictCursor)

	def emit_record(self, record):

		fields = []
		values = []

		for key, value in record.iteritems():
			fields.append(key)
			values.append(value)

		ph = ", ".join('%s' for i in xrange(len(values)))

		stmt = """INSERT INTO %s (%s) VALUES (%s)""" % (self._table, ", ".join(fields), ph)
		self._cursor.execute(stmt, values)


	def done(self):
		self._cursor.close()
		self._conn.commit()
		self._conn.close()

if __name__ == '__main__':

	run_recipe(
		ScoreSource(),
		OccupationScoreFilter(),
		MySQLEmitter('scores'),
		emitters.CountEmitter(every=100),
		#emitters.DebugEmitter(),
		error_stream = emitters.DebugEmitter(),
	)

	# run_recipe(
	# 	OccupationScoreSource(),
	# 	MySQLEmitter('scores_locations'),
	# 	emitters.CountEmitter(every=100),
	# 	#emitters.DebugEmitter(),
	# 	error_stream = emitters.DebugEmitter(),
	# )
