import re
from collections import defaultdict
district_type_dict = defaultdict(lambda:(),{
        'congressional_district':('congress','congressional','congressional district'),
        'state_senate_district':('state senate','senate','senator','state senator'),
        'state_rep_district':('state house','state representative','state rep','legislature','state legislature','legislative'),
        'county_council':('county council','county commissioner','county commission','county committee'),
        'county':('county council','county commissioner','county commission','county committee'),
        'state':('senate','senator','governor'),
        'judicial_district':('judicial district','judicial','court','appeals','supreme court'),
        'legislative_district':('state house','state representative','state rep','legislature','state legislature', 'legislative'),
        'school_district':('school','board of education','education'),
        'township':('township','town'),
        'ward':('ward'),
        })
office_names = ('delegate','congressman','congresswoman','senator','sherrif','fiscal officer','representative','judge','clerk','member','solicitor','supervisor','commissioner','state attorney','district attorney','superintendent','trustee','treasurer','mayor','attorney general','magistrate','councillor','assessor',)
from state_map import state_map
name_pat = re.compile(r'(?P<first>\w+)\s+(?:\w\.?\s+)?(:?"(?P<nick>\w+)"\s+)?(?P<last>\w+)(?:\s+jr\.|ii|iii|iv|sr\.)?')
initial_first_pat = re.compile(r'(?:\w\.?)\s+(?P<first>\w+)\s+(:?"(?P<nick>\w+)"\s+)?(?P<last>\w+)(?:\s+jr\.|ii|iii|iv|sr\.)?')
def clean_name(name):
    try:
        name = name.lower().decode('utf-8').strip()
    except UnicodeEncodeError:
        name = name.lower().strip()
    m = initial_first_pat.match(name)
    if not m:
        m = name_pat.match(name)
    if m:
        if m.groupdict()['nick']:
            name = '{first} {last}'.format(first=m.groupdict()['nick'].strip(),last=m.groupdict()['last'])
            last = m.groupdict()['last']
            first = m.groupdict()['nick']
        else:
            name = '{first} {last}'.format(first=m.groupdict()['first'],last=m.groupdict()['last'])
            last = m.groupdict()['last']
            first = m.groupdict()['first']
    else:
        name = re.split(r'\s+',name.strip())[0].strip() + ' ' + re.split(r'\s+',name.strip())[-1].strip()
        last = re.split(r'\s+',name.strip())[-1].strip()
        first = re.split(r'\s+',name.strip())[0].strip()
    return name, last, first

def search_to_feature_key(search):
    return ''.join(re.split(r'\s+', search)) + 'biptermspecial'
