from pysal.cg.shapes import Point, Polygon
from saucebrush import run_recipe
from saucebrush.emitters import SqliteEmitter, DebugEmitter
from saucebrush.filters import FieldAdder, FieldRemover, FieldRenamer, Filter
from saucebrush.sources import CSVSource
import csv
import os

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
NCES_ROOT = os.path.join(PROJECT_ROOT, 'data', 'nces')

DB = os.path.join(PROJECT_ROOT, 'data', 'load_nces.db')
HEADERS = ('school_id','name','street','city','state','zipcode','grade_low','grade_high','code')

MANUAL_GEOCODE = {
    'AURORA, OH 44202': ('41.317549', '-81.345387'),
    'BAIROIL, WY 82322': ('42.24436', '-107.55951'),
    'BELLINGHAM, WA 98229': ('48.759545', '-122.488214'),
    'BROWNSBORO, TX 75756': ('32.30235', '-95.61357'),
    'CASSOPOLIS, MI 49031': ('41.911711', '-86.009998'),
    'CINCINNATI, OH 45220': ('39.145204', '-84.516973'),
    'DORAVILLE, GA 30360': ('33.898152', '-84.283260'),
    'EL SOBRANTE, CA 94803': ('37.977138', '-122.295245'),
    'GOODRICH, ND 58444': ('47.47499', '-100.12623'),
    'LACONIA, NH 03246': ('43.527854', '-71.470351'),
    'LOCKLAND, OH 45215': ('39.229221', '-84.457715'),
    'LOREAUVILLE, LA 70552': ('30.056591', '-91.737060'),
    'MENTONE, IN 46539': ('41.17338', '-86.03471'),
    'NEW IBERIA, LA 70560': ('30.003539', '-91.818725'),
    'PLANO, TX 75093': ('33.039794', '-96.802946'),
    'PREMONT, TX 78375': ('27.360593', '-98.123612'),
    'SEYMOUR, MO 65746': ('37.14644', '-92.76877'),
    'ST LOUIS, MO 63115': ('38.678950', '-90.238538'),
    'TROUT, LA 71371': ('31.71111', '-92.25319'),
}

class GeoCoder(Filter):
    
    def __init__(self):
        
        self._geo = {}
        
        def make_points(s):
            for p in s.split('|'):
                ll = p.split(',')
                yield (float(ll[1]), float(ll[0]))
                
        outfile = open('/Users/Jeremy/Desktop/locations/city_state_msa.csv', 'w')
        writer = csv.writer(outfile)
        writer.writerow(('key','code'))
        
        for loc_record in csv.DictReader(open('/Users/Jeremy/Desktop/locations/locations.csv')):
            
            shape = Polygon([p for p in make_points(loc_record['points'])])
            bb = shape.bounding_box
            
            print "---- [%s] %s" % (loc_record['code'], loc_record['name'])
            print "     (%s, %s) (%s, %s)" % (bb.left, bb.lower, bb.right, bb.upper)
        
            infile = open('/Users/Jeremy/Desktop/locations/schools.csv')
        
            for school_record in csv.DictReader(infile):
                
                key = self.get_key(school_record)
                
                p = Point((float(school_record['latitude']), float(school_record['longitude'])))
                
                if key not in self._geo and shape.contains_point(p):
                    self._geo[key] = loc_record['code']
                    writer.writerow((key, loc_record['code']))
                    print "%s contains %s" % (loc_record['code'], key)
            
            infile.close()

        outfile.close()
            
    def get_key(self, record):
        return "%s, %s %s" % (record['city'], record['state'], record['zipcode'])
    
    def process_record(self, record):
        key = self.get_key(record)
        code = self._geo.get(key, None)
        if code is None:
            print "!!! %s, %s %s has no location" % (record['city'], record['state'], record['zipcode'])
        record['code'] = code
        return record

def load_schools():

	for filename in ('sc091aai.csv','sc091akn.csv','sc091aow.csv'):
		run_recipe(
			CSVSource(open(os.path.join(NCES_ROOT, filename))),
			FieldRemover((
				'mzip409',
				'member09',
				'phone09',
				'ulocal09',
				'type09',
				'level09',
				'status09',
			)),
			FieldRenamer({
				'school_id': 'ncessch',
				'name': 'schnam09',
				'street': 'mstree09',
				'city': 'mcity09',
				'state': 'mstate09',
				'zipcode': 'mzip09',
				'grade_low': 'gslo09',
				'grade_high': 'gshi09',
			}),
			FieldAdder('code', None),
			GeoCoder(),
			SqliteEmitter(DB, 'nces_schools', fieldnames=HEADERS),
			#DebugEmitter(),
		)

if __name__ == '__main__':

	print "Loading schools..."
	load_schools()

	print "Done!"