import csv
import urllib

APP_ID = 'Kv3.btLV34EuebZGMzi1KaqI_BOPhPjx7FtbvED.umr8DGUq0NysoGN0XIIIDRU-'

def zipcodes():
    # from http://en.wikipedia.org/wiki/List_of_ZIP_code_prefixes
    # as of 2011-09-16
    INVALID_PREFIXES = (
        '000','001','002','003','004','099','213','269','343',
        '345','348','353','419','428','429','517','518','519',
        '529','533','536','552','568','578','579','589','621',
        '632','642','643','659','663','682','694','695','696',
        '697','698','699','702','709','715','732','742','771',
        '817','818','819','839','848','849','854','858','861',
        '862','866','867','868','869','876','886','887','888',
        '892','896','899','909','929','987'
    )
    for zipcode in ("%05i" % z for z in range(100000)):
        if zipcode[:3] not in INVALID_PREFIXES:
            yield zipcode

for z in zipcodes():
    pass