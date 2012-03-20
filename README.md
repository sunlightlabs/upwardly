# Sunlight Foundation's Moving Up

Has all data:

  {
    oes: {$exists: true},
    ffiec: {$exists: true},
    nces: {$exists: true},
    rpp_local: {$exists: true},
    rpp_state: {$exists: true},
    naccrra: {$exists: true}
  }

RESULT SET:

{
    'location': {
        'code': '00000',
        'name': 'Washington, DC',
        'scores': {
            'occupation': -2,
            'qol': 3,
            'housing': 0,
            ...
        },
    },
    'compare_to': {
        'code': '00000',
        'name': 'Winchester, VA'
        'scores': {
            ...
        }
    },
    'weights': [...]
    'data': {
        'occupation': {
            'id': '00-0000',
            'name': 'Machinists',
            'category': 'Mechanical and Industrial Workers',
            'value': 45000,
            'compare_to': {
                'value': 28000,
                'cmp': -1,
            }
            'usa': {
                'value': 80000,
                'cmp': 1,
            }
        },
        'rpp_state_education_goods': {
            'value': 45000,
            'compare_to': {
                'value': 28000,
                'cmp': -1,
            }
            'usa': {
                'value': 80000,
                'cmp': 1,
            }
        },
        ...
    },
}