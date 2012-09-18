from django import forms
from django.forms.widgets import CheckboxSelectMultiple
from humanity.forms import HumanityForm
from movingup.occupations import OCCUPATIONS

EMPTY_CHOICE = [('', '------')]

WEIGHT_CHOICES = (
    ('occupation_weight', 'Salary'),
    ('childcare_weight', 'Cost of childcare'),
    ('food_weight', 'Price of food'),
    ('transportation_weight', 'Price of gas'),
    ('housing_weight', 'Price of housing'),
)


def preferences_form(zipcode=None, category_id=None, occupation_id=None, weights=None):

    categories = EMPTY_CHOICE + [(k, v['name']) for k, v in OCCUPATIONS.iteritems() if k != 0]
    categories.sort(lambda x, y: cmp(x[1], y[1]))

    fields = {
        'zipcode': forms.CharField(max_length=5, initial=zipcode),
        'category': forms.ChoiceField(choices=categories, initial=category_id),
        'occupation': forms.ChoiceField(choices=EMPTY_CHOICE),
        'weights': forms.MultipleChoiceField(choices=WEIGHT_CHOICES, widget=CheckboxSelectMultiple, required=False, initial=weights)
    }

    if category_id and occupation_id:

        occupations = EMPTY_CHOICE + [(occ['id'], occ['name']) for occ in OCCUPATIONS[int(category_id)]['occupations']]
        fields['occupation'] = forms.ChoiceField(choices=occupations, initial=occupation_id)

    return type('PreferencesForm', (forms.BaseForm,), {'base_fields': fields})


class ContactForm(HumanityForm):
    reason = forms.CharField(label="Reason", required=False)
    email = forms.EmailField(label="Email", required=False)
    comment = forms.CharField(label="Comment", widget=forms.Textarea, required=False)
