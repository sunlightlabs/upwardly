import os
import shapefile

PWD = os.path.abspath(os.path.dirname(__file__))
DESTPATH = os.path.abspath(os.path.join(PWD, '..', 'data', 'shapes'))

reader = shapefile.Reader('/Users/Jeremy/Downloads/_/cb99_03c_shp/cb99_03c')

def part_iter(shape):

	for i, start in enumerate(shape.parts):

		if i + 1 >= len(shape.parts):
			yield shape.points[start:]
		else:
			end = shape.parts[i + 1]
			yield shape.points[start:end]


for record in reader.shapeRecords():

	code = record.record[0][:5]

	print code

	writer = shapefile.Writer(shapefile.POLYGON)
	writer.poly(list(part_iter(record.shape)))

	path = os.path.join(DESTPATH, code, code)
	writer.save(path)