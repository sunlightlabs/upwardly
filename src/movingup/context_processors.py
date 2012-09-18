from movingup.forms import ContactForm


def contact_form(request):
    if request.method == 'GET':
        return {"contact_form": ContactForm()}
