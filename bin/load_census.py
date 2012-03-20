import csv
import sys

from pymongo import Connection
import settings

PATH = settings.dataset_path('aff', 'DEC_10_DP_DPDP1_with_ann.csv')

def list_columns():

    with open(PATH) as infile:

        reader = csv.reader(infile)
        rows = [reader.next() for i in xrange(1, 10)]
        columns = zip(*rows)

        writer = csv.writer(sys.stdout)
        writer.writerow(('column', 'S', 'category', 'title', 'HD', 'type'))

        for i, col in enumerate(columns):

            writer.writerow((
                i + 1,
                col[0],
                col[1],
                " ".join(col[2:7]),
                col[7],
                col[8],
            ))

def load_data():

    mongo = Connection()
    locs = mongo[settings.MONGO_DATABASE]['locations']

    with open(PATH) as infile:

        reader = csv.reader(infile)

        for i in xrange(0, 10):
            reader.next()

        for row in reader:

            code = row[1]

            loc = locs.find_one({'code': code})

            if loc:

                data = {
                    'population': int(row[3]),
                    'median_age': float(row[41]),
                    'with_children': float(row[306]),
                    'vacant_housing': float(row[344]),
                }

                loc['census'] = data
                locs.save(loc)

            else:

                print "[%s] %s not found" % (row[1], row[2])



if __name__ == "__main__":

    #list_columns()
    load_data()

