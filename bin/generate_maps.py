import os
import matplotlib

matplotlib.use('Agg')

from mpl_toolkits.basemap import Basemap
import matplotlib.pyplot as plt
import pymongo
import shapefile

import settings

class MapGenerator(object):

    def __init__(self, **kwargs):

        self.db = kwargs.pop('db', settings.MONGO_DATABASE)

        # self.shapes = {}
        # reader = shapefile.Reader('/Users/Jeremy/Downloads/_/cb99_03c_shp/cb99_03c')

        # for record in reader.shapeRecords():

        #     code = record.record[0][:5]

        #     if code not in self.shapes:
        #         self.shapes[code] = []

        #     self.shapes[code].append([list(p) for p in record.shape.points])

        super(MapGenerator, self).__init__(**kwargs)

    def generate_map(self, location):

        path = os.path.join(settings.PROJ_DIR, 'www', 'maps', '%s.png' % location['code'])

        figprops = {
            'dpi': 200,
            'facecolor': '#a5bfdd',
            'figsize': (8,8),
            'frameon': False,
        }
        fig = plt.figure(1, **figprops)
        fig.clear()

        plt.cla()

        ax = fig.add_axes((0,0,1,1))

        for sp in ax.spines.itervalues():
            sp.set_linewidth(0.0)

        centroid = location['geo']['centroid']

        map_config = {

            'projection': 'lcc',
            'resolution': 'h',
            
            'width': 200000, # map width in meters
            'height': 200000, # map height in meters
            'lat_0': centroid[0], # latitude center of map
            'lon_0': centroid[1], # longitude center of map

            # 'llcrnrlon': , # lower left longitude
            # 'llcrnrlat': , # lower left latitude
            # 'urcrnrlon': , # upper right longitude
            # 'urcrnrlat': , # upper right latitude

        }

        m = Basemap(**map_config)
        m.drawcoastlines(linewidth=0, color='#6993a6')
        m.drawmapboundary(fill_color='#a5bfdd')
        m.drawstates(linewidth=1.0, color='#7f7f7f')
        m.drawrivers(linewidth=1.0, color='#a5bfdd')
        m.fillcontinents(color='#f4f3f0', lake_color='#a5bfdd')

        m.readshapefile('/Users/Jeremy/Downloads/_/in15oc03/in101503', 'roadways', drawbounds=True, color="#999999", linewidth=1.0)

        # if location['code'] in self.shapes:
        #     for shape in self.shapes[location['code']]:
        #         poly = plt.Polygon(shape, facecolor="g", edgecolor="k", alpha=0.5)
        #         plt.gca().add_patch(poly)

        if os.path.exists(path):
            os.unlink(path)

        #fig.draw()
        fig.savefig(path, format='png', bbox_inches='tight')

    def generate_maps(self):

        conn = pymongo.Connection()
        db = conn[self.db]

        spec = {'good_to_go': True}
        locations = [loc for loc in db.locations.find(spec)]

        for loc in locations:
            print "[%s] %s" % (loc['code'], loc['name'])
            self.generate_map(loc)


if __name__ == "__main__":

    g = MapGenerator()
    g.generate_maps()
