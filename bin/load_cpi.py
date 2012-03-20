#!/Users/Jeremy/.virtualenvs/k2/bin/python
from saucebrush import run_recipe
from saucebrush.emitters import DebugEmitter, SqliteEmitter
from saucebrush.filters import Filter, FieldModifier, FieldAdder
import csv
import os
import urllib2

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
CPI_ROOT = os.path.join(PROJECT_ROOT, 'data', 'cpi')

DB = os.path.join(PROJECT_ROOT, 'data', 'load.db')

ITEMS = {
	'AAO': 'all items',
	'SAF': 'food and beverages',
	'SAF1': 'food',
	'SAF116': 'alcoholic beverages',
	'SAH': 'housing',
	'SAR': 'recreation',
	'SETB01': 'gasoline',
	'SAT': 'transportation',
	'SAE': 'education and communication',
	'SAF116': 'intracity mass transit',
}

PERIODS = {
	'R': 'monthly',
	'S': 'semi-annual',
}

CLASSES = {
	'A': (1500001, 307006550),
	'B': (50000, 1500000),
	'C': (50000, 1500000),
	'D': (0, 49999),
}

class SeriesIDFilter(Filter):

	def process_record(self, record):

		valid_items = ITEMS.keys()

		sid = record['series_id'].rstrip()

		if sid.startswith('CUU'):

			record['periodicity'] = sid[3]
			record['area_code'] = sid[4:8]
			record['item_code'] = sid[8:]

			del record['series_id']
			del record['footnote_codes']

			if record['item_code'] in valid_items:
				return record

class MSAFilter(Filter):

	def __init__(self):

		super(MSAFilter, self).__init__()

		self._msas = {}

		path = os.path.join(PROJECT_ROOT, 'data', 'locations', 'csa.csv')
		for record in csv.reader(open(path)):
			self._msas[record[2]] = (record[0], record[1])

	def process_record(self, record):
		record['msa_code'] = self._msas.get(record['area_name'][0], None)
		return record

def local_file(path):
	fd = open(path)
	for line in fd.readlines():
		yield line.strip()
	fd.close()

def local_files(paths):
	for path in paths:
		fd = open(path)
		for line in fd.readlines():
			yield line.strip()
		fd.close()

def load_areas():

	path = os.path.join(CPI_ROOT, "cu.area")
	headers = ('area_code','area_name','msa_code','display_level','selectable','sort_sequence')

	run_recipe(
		csv.DictReader(local_file(path), delimiter='\t'),
		#FieldAdder('msa_code', None),
		#MSAFilter(),
		#SqliteEmitter(DB, 'cpi_areas', fieldnames=headers),
		DebugEmitter(),
	)

def load_items():

	path = os.path.join(CPI_ROOT, "cu.item")
	headers = ('item_code','item_name','display_level','selectable','sort_sequence')

	run_recipe(
		csv.DictReader(local_file(path), delimiter='\t'),
		#SqliteEmitter(DB, 'cpi_items', fieldnames=headers),
		DebugEmitter(),
	)

def load_prices():

	paths = [os.path.join(CPI_ROOT, fn) for fn in (
		"cu.data.3.AsizeNorthEast",
		"cu.data.4.AsizeNorthCentral",
		"cu.data.5.AsizeSouth",
		"cu.data.6.AsizeWest",
		"cu.data.7.OtherNorthEast",
		"cu.data.8.OtherNorthCentral",
		"cu.data.9.OtherSouth",
		"cu.data.10.OtherWest",
	)]

	headers = ('area_code','item_code','year','periodicity','period','value')

	run_recipe(
		csv.DictReader(local_files(paths), delimiter='\t'),
		FieldModifier('value', lambda x: x.lstrip()),
		SeriesIDFilter(),
		#SqliteEmitter(DB, 'cpi_prices', fieldnames=headers),
		DebugEmitter(),
	)


if __name__ == "__main__":

    # print "Loading areas..."
    # load_areas()

    # print "Loading items..."
    # load_items()

    print "Loading prices..."
    load_prices()

    print "Done!"
