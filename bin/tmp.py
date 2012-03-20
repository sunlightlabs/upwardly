import os

from pymongo import Connection
import xlrd

BASE_PATH = '/Users/Jeremy/Downloads/bls/'

class XLSSource(object):
    
    def __init__(self, path):
        self._book = xlrd.open_workbook(path)
    
    def __iter__(self):    
        sheet = self._book.sheet_by_index(0)
        headers = [c.value.lower() for c in sheet.row(0)]
        for i in range(1, sheet.nrows):
            row = sheet.row(i)
            yield dict(zip(headers, (c.value for c in row)))
    
    def done(self):
        self._book.release_resources()

class UniqueLocationSource(object):

    files = (
        'oesm10ma/MSA_M2010_dl_1.xls',
        'oesm10ma/MSA_M2010_dl_2.xls',
        'oesm10ma/MSA_M2010_dl_3.xls',
    )

    def __init__(self):
        self.seen = set()

    def __iter__(self):    

        for file_path in self.files:

            path = os.path.join(BASE_PATH, file_path)
            source = XLSSource(path)

            for record in source:

                key = "%s%s" % (record['area'], record['area_name'])

                if key not in self.seen:
                    self.seen.add(key)
                    yield record['area'], record['area_name']


def check_missing():

    conn = Connection()
    db = conn.k2.locations

    source = UniqueLocationSource()

    for code, name in source:
        
        location = db.find_one({'code': code})

        if location is None:

            print "[%s] %s" % (code, name)

            location = db.find_one({'name': name})

            if location:
                print "   ->", location['code'], 'oes' in location

def verify_locations():

    conn = Connection()
    db = conn.k2.locations

    for code, name in UniqueLocationSource():

        location = db.find_one({'code': code})

        if location:
            if name != location['name']:
                print name, '\t', location['name']

if __name__ == '__main__':

    #check_missing()
    verify_locations()


