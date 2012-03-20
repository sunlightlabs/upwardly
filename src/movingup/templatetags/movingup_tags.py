from django import template
from django.utils import translation
import locale

locale.setlocale(locale.LC_ALL, translation.to_locale(translation.get_language()) + '.UTF-8')

register = template.Library()

@register.filter_function
def truncatechars(value, l):
	if len(value) > l:
		return value[:l] + '...'
	return value

@register.filter_function
def currency(value):
	if value is None:
		return ''
	return currency_dec(value).split('.')[0]

@register.filter_function
def currency_dec(value):
	return locale.currency(value, grouping=True)

def eval_cmp(first, second, responses):
	if second is None:
		val = first
	else:
		val = cmp(first, second)
	return responses[val + 1]

@register.simple_tag
def bw(first, second=None):
	return eval_cmp(first, second,
					('worse', 'better', 'better'))

@register.simple_tag
def bwe(first, second=None):
	return eval_cmp(first, second,
					('worse', 'equal', 'better'))

@register.simple_tag
def btwtet(first, second=None):
	return eval_cmp(first, second,
					('worse than', 'equal to', 'better than'))

@register.simple_tag
def leme(first, second=None):
	return eval_cmp(first, second,
					('more expensive', 'about as expensive', 'less expensive'))
