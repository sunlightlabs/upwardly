from django.db import connection

def salary_graph(occupation_id, location, compare_to=None):

	YEARS = ('2005','2006','2007','2008','2009','2010')

	data_loc = location['oes'].get(occupation_id, location['oes']['00-0000'])
	data_loc = {d['year']: d['mean'] for d in data_loc}

	if compare_to:

		data_cmp = compare_to['oes'].get(occupation_id, compare_to['oes']['00-0000'])
		data_cmp = {d['year']: d['mean'] for d in data_cmp}

		stats = [(y, data_loc.get(y, None), data_cmp.get(y, None)) for y in YEARS]

	else:

		stats = [(y, data_loc.get(y, None)) for y in YEARS]

	return stats

def rpp_graph(field, occupation_id, location, compare_to=None):

    cursor = connection.cursor()

    stats = {
        '00000': None,
        location['code']: None,
    }

    if compare_to:

        stats[compare_to['code']] = None

        stmt = """SELECT code, %s FROM rpp WHERE code = %%s OR code = %%s OR code = %%s""" % field
        params = ('00000', location['code'], compare_to['code'])

    else:

        stmt = """SELECT code, %s FROM rpp WHERE code = %%s OR code = %%s""" % field
        params = ('00000', location['code'])

    cursor.execute(stmt, params)

    for row in cursor.fetchall():
        if row[0] in stats:
            stats[row[0]] = float(row[1]) / 100

    cursor.close()

    if compare_to:
        return ['', stats[location['code']], stats[compare_to['code']]]
    else:
        return ['', stats[location['code']]]

