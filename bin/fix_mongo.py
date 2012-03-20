import pymongo
import settings

def fix_naccrra():

	fields = ('center_infant','center_4','center_school',
			  'family_infant','family_4','family_school')

	conn = pymongo.Connection()
	db = conn[settings.MONGO_DATABASE]

	docs = db.locations.find({'naccrra': {'$exists': 1}})

	for doc in docs:

		print doc['name']

		for field in fields:

			if field in doc['naccrra']:

				val = doc['naccrra'][field]

				if isinstance(val, basestring):

					val = val.replace('$', '')
					val = val.replace(',', '')

					try:
						val = int(val)
					except ValueError:
						val = None

					print '\t%s -> %s' % (doc['naccrra'][field], val) 

					doc['naccrra'][field] = val
					
		db.locations.save(doc)

if __name__ == '__main__':

	fix_naccrra()