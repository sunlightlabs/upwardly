from flask import Flask, Response, g, render_template, request
from pymongo import Connection, json_util
import geojson
import json
import os
import re

app = Flask(__name__)

GEOJSON_PATH = "/Users/Jeremy/Desktop/locations/geojson/"

def get_shapes(points):
    shape_points = []
    for point in points:
        if point in shape_points:
            shape_points.append(point)
            yield shape_points
            shape_points = []
        else:
            shape_points.append(point)

@app.before_request
def before_request():
    mongo_conn = Connection()
    g.mongo = mongo_conn['k2']['locations']

@app.route("/")
def index():
    return render_template("map.html")

@app.route("/msa")
def msa_list():
    
    fields = { "_id": 0, "code": 1, "name": 1 }
    ordering = [("name", 1)]
    
    if 'q' in request.args:
        query = request.args['q']
        if query.startswith('state:'):
            sel = { "primary_state": query[6:].upper() }
        else:
            regex = re.compile(query, re.I)
            sel = { "name": regex }
    else:
        sel = {}
        
    msas = [doc for doc in g.mongo.find(sel, fields, sort=ordering)]
    data = json.dumps(msas, default=json_util.default)
        
    return Response(data, mimetype='application/json')

@app.route("/msa/<code>")
def msa_detail(code):
    
    data = g.mongo.find_one({"code": code})
    
    if data and 'geo' in data:
        
        path = os.path.join(GEOJSON_PATH, "%s.json" % code)
        shape = geojson.load(open(path))
        
        data['geo']['shapes'] = [s for s in get_shapes(shape['coordinates'])]
    
    data = json.dumps(data, default=json_util.default)
        
    return Response(data, mimetype='application/json')

if __name__ == "__main__":
    app.run(debug=True, port=8000)