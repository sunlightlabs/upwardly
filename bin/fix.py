import csv

from pymongo import Connection
from saucebrush import run_recipe, stats, emitters
import MySQLdb
import settings

def calculate_diff(locs):
	for loc in locs:
		if 'ffiec' in loc:
			loc['ffiec']['diff'] = loc['ffiec']['high'] - loc['ffiec']['low']
			print loc['ffiec']
			conn.k2.locations.save(loc)

def calculate_average(locs):

	def locsource():
		for loc in locs:
			if 'ffiec' in loc and 'diff' in loc['ffiec']:
				yield loc['ffiec']

	sd = stats.StandardDeviation('diff')

	run_recipe(
		locsource(),
		sd,
	)

	print "Average: %s" % sd.average()
	print "stddev:  %s" % sd.value()[0]

def update_mysql(locs):

	conn = MySQLdb.connect(
		user=settings.MYSQL_USER,
		passwd=settings.MYSQL_PASS,
		db=settings.MYSQL_DATABASE,
		host=settings.MYSQL_HOST,
		port=settings.MYSQL_PORT,
	)

	cursor = conn.cursor()

	for loc in locs:

		if 'ffiec' in loc and 'diff' in loc['ffiec']:
		
			stmt = """UPDATE locations SET ffiec_avg = %s WHERE code = %s"""
			cursor.execute(stmt, (loc['ffiec']['diff'], loc['code']))

	cursor.close()

	conn.commit()
	conn.close()

def rescore(locs):

	conn = MySQLdb.connect(
		user=settings.MYSQL_USER,
		passwd=settings.MYSQL_PASS,
		db=settings.MYSQL_DATABASE,
		host=settings.MYSQL_HOST,
		port=settings.MYSQL_PORT,
	)

	cursor = conn.cursor()

	stmt = """SELECT AVG(ffiec_avg), STDDEV_POP(ffiec_avg) FROM locations"""
	cursor.execute(stmt)

	(avg, stddev) = [float(d) for d in cursor.fetchone()]

	stmt = """UPDATE locations SET ffiec_avg = %s WHERE code = '00000'"""
	cursor.execute(stmt, (avg,))

	for loc in locs:
		if 'ffiec' in loc and 'diff' in loc['ffiec']:

			diff = float(loc['ffiec']['diff'] - avg)
			score = int(diff / stddev)

			stmt = """UPDATE scores SET ffiec_avg = %s WHERE code = %s"""
			cursor.execute(stmt, (score, loc['code']))

	stmt = """UPDATE scores SET occupation_score = ((ffiec_avg + ffiec_high + ffiec_low) / 3) + (bls_mean * 3)"""
	cursor.execute(stmt)

	cursor.close()

	conn.commit()
	conn.close()

if __name__ == '__main__':

	conn = Connection()
	locs = conn.k2.locations.find({'good_to_go': True})

	#calculate_diff(locs)
	#calculate_average(locs)
	#update_mysql(locs)
	rescore(locs)