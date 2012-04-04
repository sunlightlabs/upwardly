from movingup.locations.models import location_from_zip, get_occupation_name, DEFAULT_WEIGHTS

class Preferences(dict):

    version = '1.0'

    @property
    def category_id(self):
        if 'category' in self and self['category']:
            return self['category']['id']

    @property
    def occupation_id(self):
        if 'occupation' in self and self['occupation']:
            return self['occupation']['id']


class PreferencesMiddleware(object):
    """ PreferencesMiddleware is used to reset and update preferences
        as well as put the current user preferences into the current
        request object.

        The `resetprefs` GET parameter will cause the current session to be
        reset to default.
    """

    def process_request(self, request):

        # delete current preferences and reset to default

        if 'resetprefs' in request.GET and 'preferences' in request.session:
            del request.session['preferences']

        # if user preferences are of an older version, replace with default
        # they'll just have to deal

        if 'preferences' not in request.session or request.session['preferences'].get('version', None) != Preferences.version:
            request.session['preferences'] = Preferences({
                'version': Preferences.version,
                'current_location': None,
                'category': None,
                'occupation': None,
                'weights': DEFAULT_WEIGHTS,
            })

        # POST is intended to update user preferences

        if 'update_preferences' in request.POST:

            if 'zipcode' in request.POST:

                zipcode = request.POST['zipcode']
                loc = location_from_zip(zipcode)

                if loc:

                    request.session['preferences']['location'] = {
                        'zipcode': request.POST['zipcode'],
                        'name': loc['name'],
                        'id': loc['code'],
                    }

            if 'occupation' in request.POST:

                occ_id = request.POST['occupation']
                cat_id = occ_id.split('-')[0]

                if occ_id and cat_id:

                    request.session['preferences']['category'] = {
                        'id': cat_id,
                    }
                    request.session['preferences']['occupation'] = {
                        'id': occ_id,
                        'category': cat_id,
                        'name': get_occupation_name(occ_id) or 'Hard Worker',
                    }

            request.session['preferences']['weights'] = DEFAULT_WEIGHTS.copy()
            request.session['preferences']['selected_weights'] = None

            if 'weights' in request.POST:

                selected_weights = request.POST.getlist('weights')

                request.session['preferences']['selected_weights'] = selected_weights

                for weight, default_value in DEFAULT_WEIGHTS.iteritems():

                    if weight in selected_weights:
                        request.session['preferences']['weights'][weight] = default_value * 2
                    else:
                        request.session['preferences']['weights'][weight] = default_value

            request.session.modified = True

        request.preferences = request.session['preferences']

