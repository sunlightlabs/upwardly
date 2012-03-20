from django.db import connection
from django.http import HttpResponse, HttpResponseRedirect
from django.shortcuts import get_object_or_404, render
from django.utils.functional import wraps
from pymongo import json_util
import json

from movingup import charts
from movingup.data import db
from movingup.locations.models import (
    cached_compare, all_locations, get_occupation_name,
    location_score, location_scores, nearby_locations, nearby_zipcodes)

def get_object_or_none(model, **kwargs):
    try:
        return model.objects.get(**kwargs)
    except model.DoesNotExist:
        pass # just fall through to returning None

# index, search result view

def index(request):

    if 'location' not in request.session:
        #return HttpResponseRedirect('/locations/filter/')
        request.session['location'] = '10820'

    return render(request, 'locations/index.html')

def browse(request):

    context = {
        'occupation': request.preferences.get('occupation', None)
    }

    compare_code = request.GET.get('compareto', None)

    if 'lat' in request.GET and 'lng' in request.GET:
        locations = nearby_locations(request.GET['lat'], request.GET['lng'], excluding=compare_code, limit=25)
        context['near'] = {
            'lat': request.GET['lat'],
            'lng': request.GET['lng'],
        }
    else:
        locations = all_locations(excluding=compare_code)

    if compare_code:
        compare_to = db.k2.locations.find_one({'code': compare_code})
    else:
        compare_to = None

    context['compare_to'] = compare_to
    context['locations'] = locations

    return render(request, 'locations/browse.html', context)

# apply filters for occupation, etc.

def filter(request):

    if request.method == 'POST':
        pass

    occupation_id = request.GET.get('occ', None) or request.preferences.occupation_id
    pref_location = request.preferences['location']

    if pref_location is None:
        # set message
        pass # redirect to preferences

    weights = request.preferences['weights']

    locations = location_scores(occupation_id, pref_location['id'], weights)
    score = location_score(occupation_id, pref_location['id'], weights)

    occupation = {
        'id': occupation_id,
        'name': get_occupation_name(occupation_id),
    }

    return render(request, 'results.html', {
        'locations': locations,
        'current_location': pref_location,
        'score': score,
        'occupation': occupation,
    })

# find nearby locations and zipcodes

def nearby(request):

    limit = int(request.GET['limit']) if 'limit' in request.GET else 10

    if 'lat' in request.GET and 'lng' in request.GET:

        locations = nearby_locations(request.GET['lat'], request.GET['lng'], limit=limit)

        return HttpResponse(
            json.dumps(locations, default=json_util.default),
            mimetype='application/json')

    return HttpResponseRedirect('/locations/')

def zipcodes(request):

    if 'lat' in request.GET and 'lng' in request.GET:

        zipcodes = nearby_zipcodes(request.GET['lat'], request.GET['lng'])

        return HttpResponse(json.dumps(zipcodes, default=json_util.default), mimetype='application/json')

    return HttpResponseRedirect('/locations/')

# location specific views

def location_view(view):
    @wraps(view)
    def inner(request, msa, *args, **kwargs):

        kwargs = {}

        if 'occ' in request.GET and request.GET['occ']:
            occupation_id = request.GET['occ']
        else:
            occupation_id = request.preferences.occupation_id

        kwargs['occupation_id'] = occupation_id

        location = db.k2.locations.find_one({'code': msa})
        #location['scores'] = location_score(occupation_id, msa)

        compare_to = None

        if 'compareto' in request.GET:
            compare_msa = request.GET['compareto']
            if compare_msa != msa:
                compare_to = db.k2.locations.find_one({'code': compare_msa})
                #compare_to['scores'] = location_score(occupation_id, compare_msa)

        weights = request.preferences['weights']

        comparison = cached_compare(occupation_id, location, compare_to, weights)

        return view(request, location, compare_to, comparison, **kwargs)
    return inner


@location_view
def overview(request, location, compare_to, comparison, **kwargs):

    occupation_id = kwargs.get('occupation_id', None)

    if not occupation_id:
        occupation_id = request.preferences.occupation_id
        occupation = request.preferences['occupation']
    else:
        occupation = {
            'id': occupation_id,
            'name': get_occupation_name(occupation_id),
        }

    chart_data = {
        'salary': json.dumps(charts.salary_graph(occupation_id, location, compare_to)),
        'housing': json.dumps(charts.rpp_graph('housing_goods', occupation_id, location, compare_to)),
        'education': json.dumps(charts.rpp_graph('education_services', occupation_id, location, compare_to)),
        'recreation': json.dumps(charts.rpp_graph('recreation_services', occupation_id, location, compare_to)),
    }

    context = {
        'location': location,
        'compare_to': compare_to,
        'comparison': comparison,
        'occupation': occupation,
        'chart_data': chart_data,
    }
    return render(request, 'locations/overview.html', context)

@location_view
def employment(request, location, compare_to, comparison, **kwargs):
    context = {'location': location, 'compare_to': compare_to, 'comparison': comparison}
    return render(request, 'locations/employment.html', context)

@location_view
def finances(request, location, compare_to, comparison, **kwargs):
    context = {'location': location, 'compare_to': compare_to, 'comparison': comparison}
    return render(request, 'locations/transportation.html', context)

@location_view
def housing(request, location, compare_to, comparison, **kwargs):
    context = {'location': location, 'compare_to': compare_to, 'comparison': comparison}
    return render(request, 'locations/housing.html', context)

@location_view
def lifestyle(request, location, compare_to, comparison, **kwargs):
    context = {'location': location, 'compare_to': compare_to, 'comparison': comparison}
    return render(request, 'locations/lifestyle.html', context)