#!/Users/Jeremy/.virtualenvs/spots/bin/python
import csv
import os
import pymongo

DATA_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'data'))

mongo = pymongo.Connection()
db = mongo.sunlightspots

def type_filter(record, fields, func):
    for field in fields:
        record[field] = func(record[field])
    
def load_census():
    
    reader = csv.DictReader(open(os.path.join(DATA_ROOT, 'census.csv')))
    for record in reader:
        
        msa = record['msa']
        name = record['name']
        
        del record['msa']
        del record['name']
        
        type_filter(record, (
            'pop','pop_child','pop_inst','house','house_fam','house_fam_child',
            'house_child','house_senior','units','units_occ','units_vac',
            'units_vac_rent','units_sale','units_sold','units_vac_vacation',
            'units_vac_other','units_occ_own','units_occ_own_pop',
            'units_occ_rent','units_occ_rent_pop'), int)
        
        type_filter(record, (
            'house_size','house_fam_size','units_occ_own_size',
            'units_occ_rent_size'), float)
        
        location = db.locations.find_one({'msa': int(msa)})
        
        if location:
            print "%s" % unicode(name, 'ascii', errors='ignore')
            location['census'] = record
            db.locations.save(location)
        else:
            print "---- unable to location %s: %s" % (msa, name)

    #db.locations.update({"census": { "$exists": False}}, {"$set": {"census": None}})

if __name__ == "__main__": 
    load_census()