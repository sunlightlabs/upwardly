import os
import re

from PIL import Image
import settings

MAP_RE = re.compile(r"\d{5}\.png")

if __name__ == "__main__":

    WIDTH = 400
    HEIGHT = 400

    maps_path = os.path.join(settings.PROJ_DIR, 'www', 'maps')

    for filename in os.listdir(maps_path):

        if not MAP_RE.match(filename):
            continue

        (name, ext) = filename.rsplit('.')
        rfilename = "%s-%sx%s.%s" % (name, WIDTH, HEIGHT, ext)

        inpath = os.path.join(maps_path, filename)
        outpath = os.path.join(maps_path, rfilename)

        img = Image.open(inpath)
        img.thumbnail((WIDTH, HEIGHT), Image.ANTIALIAS)
        img.save(outpath, "PNG")