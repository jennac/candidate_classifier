import urllib2 as ul
import re, csv, json, time, random
from state_map import state_map
from multiprocessing import Pool, Lock

name_pat = re.compile(r'(?P<first>\w+)\s+(?:\w\.?\s+)?(:?"(?P<nick>\w+)"\s+)?(?P<last>\w+)(?:\s+jr\.|ii|iii|iv|sr\.)?')
initial_first_pat = re.compile(r'(?:\w\.?)\s+(?P<first>\w+)\s+(:?"(?P<nick>\w+)"\s+)?(?P<last>\w+)(?:\s+jr\.|ii|iii|iv|sr\.)?')
def getlinks(candidate, webpage, state, district_type, district_name):
    district_type = district_type.replace('district','').replace('_',' ').strip()
    state = state_map[state.strip()]
    #candidate = '"'+('+'.join(candidate.strip().split(' ')))+'"'
    #candidate = candidate.strip().split(' ')[-1]
    candidate = candidate.lower().strip()
    m = initial_first_pat.match(candidate)
    if not m:
        m = name_pat.match(candidate)
    if m:
        if m.groupdict()['nick']:
            candidate = '{first}+{last}'.format(first=m.groupdict()['nick'].strip(),last=m.groupdict()['last'])
        else:
            candidate = '{first}+{last}'.format(first=m.groupdict()['first'],last=m.groupdict()['last'])
    else:
        print 'no match'
        candidate = '"' + candidate.strip().split(' ')[0] + '+' + candidate.strip().split(' ')[-1] + '"'
    print candidate
    state = '+'.join(state.split(' '))
    district_type = '+'.join(district_type.split(' '))
    district_name = '+'.join(district_name.strip().split(' '))
    search_urls = []
    #search_urls.append('https://www.googleapis.com/customsearch/v1?cx=011743744063680272768:2oildgpr9n0&key=AIzaSyCdHlGJuMzGBH9hNsEMObffDIkzJ44EQhA&hl=en&q={name}+{state}+2012'.format(name=candidate, state=state, district_type=district_type))
    search_urls.append('https://www.googleapis.com/customsearch/v1?cx=011743744063680272768:2oildgpr9n0&key=AIzaSyCdHlGJuMzGBH9hNsEMObffDIkzJ44EQhA&hl=en&q={name}+{state}+donate+contribute+volunteer'.format(name=candidate, state=state, district_type=district_type))
    search_urls.append('https://www.googleapis.com/customsearch/v1?cx=011743744063680272768:2oildgpr9n0&key=AIzaSyCdHlGJuMzGBH9hNsEMObffDIkzJ44EQhA&hl=en&q={name}+{state}'.format(name=candidate, state=state, district_type=district_type))
    search_urls.append('https://www.googleapis.com/customsearch/v1?cx=011743744063680272768:2oildgpr9n0&key=AIzaSyCdHlGJuMzGBH9hNsEMObffDIkzJ44EQhA&hl=en&q={name}+{state}+volunteer'.format(name=candidate, state=state, district_type=district_type))
    search_urls.append('https://www.googleapis.com/customsearch/v1?cx=011743744063680272768:2oildgpr9n0&key=AIzaSyCdHlGJuMzGBH9hNsEMObffDIkzJ44EQhA&hl=en&q={name}+{state}+{district_type}+2012'.format(name=candidate, state=state, district_type=district_type))
    search_urls.append('https://www.googleapis.com/customsearch/v1?cx=011743744063680272768:2oildgpr9n0&key=AIzaSyCdHlGJuMzGBH9hNsEMObffDIkzJ44EQhA&hl=en&q={name}+{state}+{district_type}+campaign+2012'.format(name=candidate, state=state, district_type=district_type))
    search_urls.append('https://www.googleapis.com/customsearch/v1?cx=011743744063680272768:2oildgpr9n0&key=AIzaSyCdHlGJuMzGBH9hNsEMObffDIkzJ44EQhA&hl=en&q={name}+{state}+{district_type}+{district_name}+2012'.format(name=candidate, state=state, district_type=district_type, district_name=district_name))
    search_urls.append('https://www.googleapis.com/customsearch/v1?cx=011743744063680272768:2oildgpr9n0&key=AIzaSyCdHlGJuMzGBH9hNsEMObffDIkzJ44EQhA&hl=en&q={name}+{state}+{district_type}+{district_name}+campaign+2012'.format(name=candidate, state=state, district_type=district_type, district_name=district_name))
    #print search_urls
    websites = []
    webpage_stripped = webpage.lstrip('http://').lstrip('www.').rstrip('/')
    patt = re.compile(r'^http://(?:www.)?{webpage}/?$'.format(webpage=webpage_stripped.lower()))
    n = 1
    while True:
        try:
            results = map(lambda x: json.loads(ul.urlopen(x).read()),search_urls)
            break
        except ul.HTTPError as error:
            if error.getcode() == 403:
                if n > 16:
                    raise error
                print 'sleeping'
                time.sleep(n + random.randint(1,1000)/1000.)
                n = n*2
            else:
                raise error

    if type(results) != list:
        print type(results)
        results = [results]
    real_results = [(r if r.has_key('items') else {'items':[]}) for r in results]
    results = real_results
    search_links = [[i['link'].lower() for i in r['items']] for r in results]
    websites = [any(map(patt.match,sl)) for sl in search_links]
    non_websites = [[i['link'] for i in r['items'] if webpage not in i['link']] for r in results]
    return non_websites, websites, webpage_stripped

lock = Lock()
def runit(l, uid):
    print l['candidate_url']
    try:
        non_webpage_list, search_success_vector, webpage = getlinks(l['name'].decode('utf-8').strip(), l['candidate_url'].decode('utf-8').strip(), l['state'].decode('utf-8').strip(), l['electoral_district_type'].decode('utf-8').strip(), l['electoral_district_name'].decode('utf-8').strip())
    except Exception as error:
        import traceback; print traceback.format_exc()
        print error
        return uid,[],[]
    return uid,non_webpage_list, search_success_vector, webpage

with open('webpages_ex.csv') as f, open('nonwebpages.csv','w') as g, open('webpage_ssv.csv','w') as h:
    csvr = csv.DictReader(f)
    csvw = csv.writer(g)
    csvw2 = csv.writer(h)
    csvw.writerow(['uid','non_webpage_list'])
    csvw2.writerow(['uid','webpage','search_success_vector'])
    pool = Pool(processes=10)
    def callb(results):
        uid = results[0]
        nwl = results[1]
        ssv = results[2]
        webpage = results[3]
        lock.acquire()
        csvw.writerow([uid,nwl])
        csvw2.writerow([uid,webpage,ssv])
        lock.release()
    for l in csvr:
        uid = l['identifier']
        pool.apply_async(runit, [l,uid], callback=callb)
    pool.close()
    pool.join()

