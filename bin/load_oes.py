#!/Users/Jeremy/.virtualenvs/sandbox/bin/python
from cStringIO import StringIO
from pymongo import Connection
from saucebrush.emitters import CountEmitter, DebugEmitter, CSVEmitter, SqlDumpEmitter, Emitter, SqliteEmitter
from saucebrush.filters import (ConditionalFilter, FieldFilter, FieldAdder,
    FieldModifier, FieldRemover, FieldKeeper, FieldRenamer, Filter)
from saucebrush.sources import CSVSource, FixedWidthFileSource
from saucebrush.stats import StandardDeviation, Sum
from saucebrush import utils
from saucebrush import run_recipe
import itertools
import os
import urllib2
import xlrd

MAX_SALARIES = {
    '2003': 145600,
    '2004': 145600,
    '2005': 145600,
    '2006': 145600,
    '2007': 145600,
    '2008': 166400,
    '2009': 166400,
    '2010': 166400,
}

# saucebrush utility methods/objects

def remote_files(*urls, **kwargs):
    file_count = 0
    headers = kwargs.get('headers', False)
    for url in urls:
        resp = urllib2.urlopen(url)
        if headers and file_count > 0:
            resp.next()
        for line in resp:
            yield line.rstrip()
        resp.close()
        file_count += 1

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

class SeriesIDFilter(FieldFilter):
    
    def process_record(self, record):
        
        series_id = record['series_id']
        
        record['survey_abbr'] = series_id[0:2]
        record['seasonal_code'] = series_id[2]
        record['periodicity_code'] = series_id[3]
        record['area_code'] = series_id[4:8]
        record['item_code'] = series_id[8:]
        
        return record

class ValueConditionalFilter(ConditionalFilter):
    
    def __init__(self, field, values):
        self._field = field
        self._values = utils.str_or_list(values)
    
    def test_record(self, record):
        return record[self._field] in self._values

class AvailableDataFilter(ConditionalFilter):
    values = ('*','**','***')
    def test_record(self, record):
        return record['salary_mean'] not in self.values and record['salary_median'] not in self.values

class HighRollerFilter(Filter):
    def process_record(self, record):
        max_salary = MAX_SALARIES[record['year']]
        if record['salary_median'] == '#':
            record['salary_median'] = max_salary
        if record['salary_mean'] == '#':
            record['salary_mean'] = max_salary
        return record

class OESMongoEmitter(Emitter):
    
    def __init__(self, reset=False):
        super(OESMongoEmitter, self).__init__()
        
        mongo = Connection()
        self._locations = mongo['k2']['locations']
        
        if reset:
            self._locations.update({}, {'$unset': {'oes': 1}}, multi=True)
    
    def emit_record(self, record):
        doc = self._locations.find_one({'code': record['code']})
        if doc is not None:
            occ_id = record['occupation_id']
            if 'primary_state' not in doc:
                doc['primary_state'] = record['primary_state']
            if 'oes' not in doc:
                doc['oes'] = {}
            if occ_id not in doc['oes']:
                doc['oes'][occ_id] = []
            doc['oes'][occ_id].append({
                'year': record['year'],
                'mean': record['salary_mean'],
                'median': record['salary_median'],
                'is_major': record['grp'] == 'major',
                'is_total': record['grp'] == 'total',
            })
            self._locations.save(doc)

#
# CPI loading methods
#

def load_cpi_items():
    headers = ('item_code','item_name','display_level','selectable','sort_sequence')
    url = "ftp://ftp.bls.gov/pub/time.series/cu/cu.item"
    run_recipe(
        CSVSource(utils.RemoteFile(url), delimiter='\t'),
        CSVEmitter(open('bls_items.csv', 'w'), headers)
    )

def load_cpi_areas():
    headers = ('area_code','area_name','display_level','selectable','sort_sequence')
    url = "ftp://ftp.bls.gov/pub/time.series/cu/cu.area"
    run_recipe(
        CSVSource(utils.RemoteFile(url), delimiter='\t'),
        CSVEmitter(open('bls_areas.csv', 'w'), headers)
    )

def load_cpi():
    
    urls = {
        'north_east': 'ftp://ftp.bls.gov/pub/time.series/cu/cu.data.3.AsizeNorthEast',
        'north_central': 'ftp://ftp.bls.gov/pub/time.series/cu/cu.data.4.AsizeNorthCentral',
        'south': 'ftp://ftp.bls.gov/pub/time.series/cu/cu.data.5.AsizeSouth',
        'west': 'ftp://ftp.bls.gov/pub/time.series/cu/cu.data.6.AsizeWest',
    }
    
    url = urls['west']
    headers = ('series_id','survey_abbr','seasonal_code','periodicity_code',
        'area_code','item_code','year','period','value','footnote_codes')
    
    reader = remote_files(*urls.values(), headers=True)
    
    run_recipe(
        #CSVSource(utils.RemoteFile(url), delimiter='\t'),
        CSVSource(reader, delimiter='\t'),
        FieldModifier(('series_id','value'), str.strip),
        SeriesIDFilter('series_id'),
        ValueConditionalFilter('year', '2008'),
        CSVEmitter(open('bls.csv', 'w'), headers)
    )

def load_oes():
    
    headers = ('prim_state','area','area_name','occ_code','occ_title','group',
        'tot_emp', 'emp_prse','jobs_1000','loc quotient','h_mean','a_mean',
        'mean_prse','h_pct10','h_pct25','h_median','h_pct75','h_pct90',
        'a_pct10','a_pct25','a_median','a_pct75','a_pct90','annual','hourly')
        
    BASE_PATH = '/Users/Jeremy/Downloads/bls/'
    
    dataset = {
        '2003': (
            'oesm03ma/msa_may2003_dl_1.xls',
            'oesm03ma/msa_may2003_dl_2.xls',
        ),
        '2004': (
            'oesm04ma/MSA_may2004_dl_1.xls',
            'oesm04ma/MSA_may2004_dl_2.xls',
            'oesm04ma/MSA_may2004_dl_3.xls',
        ),
        '2005': (
            'oesm05ma/MSA_may2005_dl_1.xls',
            'oesm05ma/MSA_may2005_dl_2.xls',
            'oesm05ma/MSA_may2005_dl_3.xls',
        ),
        '2006': (
            'oesm06ma/MSA_may2006_dl_1.xls',
            'oesm06ma/MSA_may2006_dl_2.xls',
            'oesm06ma/MSA_may2006_dl_3.xls',
        ),
        '2007': (
            'oesm07ma/MSA_May2007_dl_1.xls',
            'oesm07ma/MSA_May2007_dl_2.xls',
            'oesm07ma/MSA_May2007_dl_3.xls',
        ),
        '2008': (
            'oesm08ma/MSA__M2008_dl_1.xls',
            'oesm08ma/MSA_M2008_dl_2.xls',
            'oesm08ma/MSA_M2008_dl_3.xls',
        ),
        '2009': (
            'oesm09ma/MSA_dl_1.xls',
            'oesm09ma/MSA_dl_2.xls',
            'oesm09ma/MSA_dl_3.xls',
        ),
        '2010': (
            'oesm10ma/MSA_M2010_dl_1.xls',
            'oesm10ma/MSA_M2010_dl_2.xls',
            'oesm10ma/MSA_M2010_dl_3.xls',
        ),
    }
    
    counter = CountEmitter(every=100)
    mongo_emitter = OESMongoEmitter(reset=True)
    
    for year, paths in dataset.iteritems():
        
        for path in paths:
            
            print path
            
            run_recipe(
                XLSSource(os.path.join(BASE_PATH, path)),
                FieldKeeper(('prim_state','area','occ_code','occ_title','group','a_mean','a_median')),
                FieldRenamer({
                    'primary_state': 'prim_state',
                    'code': 'area',
                    'occupation_id': 'occ_code',
                    'occupation': 'occ_title',
                    'grp': 'group',
                    'salary_mean': 'a_mean',
                    'salary_median': 'a_median',
                }),
                FieldAdder('year', year),
                AvailableDataFilter(),
                HighRollerFilter(),
                FieldModifier(('salary_mean','salary_median'), int),
                #SqliteEmitter('/Users/Jeremy/Desktop/locations/oes.db', 'oes', fieldnames=('year','primary_state','code','occupation_id','occupation','grp','salary_mean','salary_median')),
                FieldRemover('occupation'),
                mongo_emitter,
                counter,
                error_stream=DebugEmitter(),
            )

if __name__ == '__main__':
    
    # print "Loading areas..."
    # load_cpi_areas()
    # 
    # print "Loading items..."
    # load_cpi_items()
    # 
    # print "Loading data..."
    # load_cpi()
    # 
    # print "Done!"
    
    load_oes()