from django.core.mail import send_mail
from django.http import HttpResponse, HttpResponseRedirect
from django.shortcuts import render
from movingup.data import db
from movingup.forms import preferences_form, ContactForm
from movingup.locations.models import compare
from movingup.occupations import OCCUPATIONS
import json


def home(request):
    return render(request, 'index.html')


def get_started(request):

    form_class = preferences_form(
        zipcode=request.preferences['location']['zipcode'] if 'location' in request.preferences else None,
        category_id=request.preferences.category_id,
        occupation_id=request.preferences.occupation_id,
        weights=request.preferences.get('selected_weights', None),
    )

    if request.method == 'POST':

        form = form_class(request.POST)

        if form.is_valid():
            return HttpResponseRedirect('/locations/filter/?occ=%s' % request.preferences.occupation_id)

    else:
        form = form_class()

    return render(request, 'preferences.html', {'form': form})


def occupations(request, category_id=None):
    if category_id:
        category = OCCUPATIONS[int(category_id)]
        data = [(occ['id'], occ['name']) for occ in category['occupations']]
    else:
        data = [(cat_id, cat['name']) for cat_id, cat in OCCUPATIONS.iteritems()]
    return HttpResponse(json.dumps(data), mimetype='application/json')


def contact(request):

    if request.method == 'POST':

        form = ContactForm(request.POST)

        print request.POST

        if form.is_valid():

            reason = request.POST.get('reason', '')
            comment = request.POST.get('comment', '')
            email = request.POST.get('email', '')

            message = "%s\n\n%s\n\n%s" % (reason, comment, email)

            send_mail(
                '[Upwardly Mobile] Contact form submission',
                message,
                'contact@sunlightfoundation.com',
                ['jcarbaugh@sunlightfoundation.com'],
                fail_silently=True
            )

    if request.is_ajax:
        return HttpResponse('', content_type='text/plain')
    else:
        return HttpResponseRedirect('/')


def debug(request):

    loc_code = '44220'  # Winchester, VA
    cmp_code = '49020'  # Springfield, OH
    occ_id = '15-1179'

    loc = db.k2.locations.find_one({'code': loc_code})
    cmp_to = db.k2.locations.find_one({'code': cmp_code})

    resp = {
        'preferences': request.preferences,
        'results': compare(occ_id, loc, compare_to=cmp_to),
    }

    return HttpResponse(json.dumps(resp), mimetype='application/json')
