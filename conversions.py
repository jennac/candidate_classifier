import re
from collections import defaultdict
from state_map import state_map
import requests
import urllib2
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

def state_from_abbr(state):
    return state_map[state]

def strip_and_std(url):
    webpage_stripped = re.match(r'(?:https?://)?(?P<content>.+)',url).groupdict()['content'].rstrip('/')
    return 'http://'+webpage_stripped

def child_page(url, child, strip=False):
    if strip:
        webpage_stripped = strip_scheme_www(strip_queries(url))
        #webpage_no_queries = urllib2.urlparse.urlparse(webpage)
        #webpage_stripped = re.match(r'(?:www\.)?(?P<content>.+)',webpage_no_queries.netloc + webpage_no_queries.path).groupdict()['content'].rstrip('/')
    else:
        webpage_stripped = strip_scheme_www(url)
        #webpage_stripped = re.match(r'(?:https?://)?(?:www\.)?(?P<content>.+)',url).groupdict()['content'].rstrip('/')
    child_patt = re.compile(r'^https?://(?:www\.)?{webpage}[/?].+'.format(webpage=re.escape(webpage_stripped.lower())))
    return child_patt.match(child)

def equal_page(url, child, strip=False):
    if strip:
        webpage_stripped = strip_scheme_www(strip_queries(url))
        #webpage_no_queries = ul.urlparse.urlparse(webpage)
        #webpage_stripped = re.match(r'(?:www\.)?(?P<content>.+)',webpage_no_queries.netloc + webpage_no_queries.path).groupdict()['content'].rstrip('/')
    else:
        webpage_stripped = strip_scheme_www(url)
        #webpage_stripped = re.match(r'(?:https?://)?(?:www\.)?(?P<content>.+)',url).groupdict()['content'].rstrip('/')
    equal_patt = re.compile(r'^https?://(?:www\.)?{webpage}/?'.format(webpage=re.escape(webpage_stripped.lower())))
    return equal_patt.match(child)

def child_or_equal_page(url, child, strip=False):
    if strip:
        webpage_stripped = strip_scheme_www(strip_queries(url))
        #webpage_no_queries = ul.urlparse.urlparse(webpage)
        #webpage_stripped = re.match(r'(?:www\.)?(?P<content>.+)',webpage_no_queries.netloc + webpage_no_queries.path).groupdict()['content'].rstrip('/')
    else:
        webpage_stripped = strip_scheme_www(url)
        #webpage_stripped = re.match(r'(?:https?://)?(?:www\.)?(?P<content>.+)',url).groupdict()['content'].rstrip('/')
    child_or_equal_patt = re.compile(r'^https?://(?:www\.)?{webpage}.*'.format(webpage=re.escape(webpage_stripped.lower())))
    return child_or_equal_patt.match(child)

def strip_queries(url):
    parsed = urllib2.urlparse.urlparse(url)
    if parsed.scheme != '':
        return parsed.scheme + '://' + parsed.netloc + parsed.path
    else:
        return parsed.netloc+parsed.path

def strip_scheme_www(url):
    webpage_stripped = re.match(r'(?:https?://)?(?:www\.)?(?P<content>.+)',url).groupdict()['content'].rstrip('/')
    return webpage_stripped

def page_relation(test_page,strip, *args):
    true_urls = filter(lambda child: not any(map(lambda url: child_page(url,(strip_queries(child) if strip else child),strip), args)),args)
    if any(map(lambda parent: child_page(parent,test_page,strip),true_urls)):
        return 'Child'
    elif any(map(lambda parent: equal_page(parent,test_page,strip),true_urls)):
        return 'True'
    elif any(map(lambda child: child_page(test_page,child,strip),true_urls)):
        return 'Parent'
    else:
        return 'False'

num_tries = 3
def get_redirect(url):
    try:
        for i in range(num_tries):
            site = requests.get(strip_and_std(url))
            if site.status_code == 200:
                redirect_url = site.url
                break
        else:
            redirect_url =  '404'
    except Exception as e:
        redirect_url = 'ERROR'
    return redirect_url

def twitter_handle_to_web(handle):
    return 'http://twitter.com/'+handle

def web_to_twitter_handle(web):
    parsed = urllib2.urlparse.urlparse(web)
    return parsed.path.split('/')[1]

def clean_twitter(url):
    return url.replace('#!/','')
