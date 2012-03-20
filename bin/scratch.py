import csv
import os

import settings

headers = ('area_code','item_code','year','periodicity','period','value')

paths = (
	"cu.data.3.AsizeNorthEast",
	"cu.data.4.AsizeNorthCentral",
	"cu.data.5.AsizeSouth",
	"cu.data.6.AsizeWest",
	"cu.data.7.OtherNorthEast",
	"cu.data.8.OtherNorthCentral",
	"cu.data.9.OtherSouth",
	"cu.data.10.OtherWest",
)

items:

for filename in paths:

	path = settings.dataset_path('cpi', filename)

	with open(path) as infile:

		for row in csv.DictReader(infile, delimiter='\t'):

			row = {k: v.strip() for (k, v) in row.iteritems()}

			sid = row['series_id']

			if not sid.startswith('CUU'):
				continue

			row['periodicity'] = sid[3]
			row['area_code'] = sid[4:8]
			row['item_code'] = sid[8:]

			del row['footnote_codes']

			print row


