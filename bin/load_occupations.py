from saucebrush import sources, filters, emitters, run_recipe
import MySQLdb
import settings
import pprint

class ValidOccupationFilter(filters.ConditionalFilter):
    def test_record(self, record):
        return record['id'] != '00-0000'

class CategoryIDFilter(filters.Filter):
    def process_record(self, record):
        record['category_id'] = int(record['id'].split('-')[0])
        return record

class CategoryEmitter(emitters.Emitter):

    def __init__(self, conn, **kwargs):
        super(CategoryEmitter, self).__init__(**kwargs)
        self._cursor = conn.cursor()
        self._stmt = """INSERT INTO occupation_category (id, name) VALUES (%s, %s)"""

    def emit_record(self, record):
        if record['id'].endswith('0000'):
            name = record['name'][:-12]
            params = (record['category_id'], name)
            self._cursor.execute(self._stmt, params)

    def done(self):
        self._cursor.close()

class OccupationEmitter(emitters.Emitter):

    def __init__(self, conn, **kwargs):
        super(OccupationEmitter, self).__init__(**kwargs)
        self._cursor = conn.cursor()
        self._stmt = """INSERT INTO occupation (id, category_id, name) VALUES (%s, %s, %s)"""

    def emit_record(self, record):
        if not record['id'].endswith('0000'):
            params = (record['id'], record['category_id'], record['name'])
            self._cursor.execute(self._stmt, params)

    def done(self):
        self._cursor.close()

def load_locations():

    conn = MySQLdb.connect(
        user=settings.MYSQL_USER,
        passwd=settings.MYSQL_PASS,
        db=settings.MYSQL_DATABASE,
        host=settings.MYSQL_HOST,
        port=settings.MYSQL_PORT,
    )

    cursor = conn.cursor()
    cursor.execute("""DELETE FROM occupation_category""")
    cursor.execute("""DELETE FROM occupation""")
    cursor.close()

    path = settings.dataset_path(None, filename='occupations.csv')

    run_recipe(
        sources.CSVSource(open(path)),
        ValidOccupationFilter(),
        CategoryIDFilter(),
        CategoryEmitter(conn),
        OccupationEmitter(conn),
        #emitters.DebugEmitter(),
        error_stream = emitters.DebugEmitter(),
    )

    conn.commit()
    conn.close()

def generate_configfile():

    conn = MySQLdb.connect(
        user=settings.MYSQL_USER,
        passwd=settings.MYSQL_PASS,
        db=settings.MYSQL_DATABASE,
        host=settings.MYSQL_HOST,
        port=settings.MYSQL_PORT,
    )

    struct = {}

    cursor = conn.cursor()

    cursor.execute("""SELECT * FROM occupation_category ORDER BY name""")
    for row in cursor:
        struct[row[0]] = {
            'name': row[1],
            'occupations': [],
        }

    cursor.execute("""SELECT * FROM occupation ORDER BY name""")
    for row in cursor:
        struct[row[1]]['occupations'].append({
            'id': row[0],
            'name': row[2]
        })

    cursor.close()
    conn.close()

    dictstr = pprint.pformat(struct)

    print "OCCUPATIONS = %s" % dictstr

if __name__ == '__main__':

    #load_locations()

    generate_configfile()