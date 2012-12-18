import urllib2 as ul
import re, csv, json, time, random
from state_map import state_map
from multiprocessing import Pool, Lock
from univ_settings import ERSATZPG_CONFIG
import chardet
from new_pages_conf import get_page_for_class

name_pat = re.compile(r'(?P<first>\w+)\s+(?:\w\.?\s+)?(:?"(?P<nick>\w+)"\s+)?(?P<last>\w+)(?:\s+jr\.|ii|iii|iv|sr\.)?')
initial_first_pat = re.compile(r'(?:\w\.?)\s+(?P<first>\w+)\s+(:?"(?P<nick>\w+)"\s+)?(?P<last>\w+)(?:\s+jr\.|ii|iii|iv|sr\.)?')
def getlinks(candidate, webpage, state, district_type, district_name):
    #district_type = district_type.replace('district','').replace('_',' ').strip()
    district_type = district_type.replace('_',' ').strip()
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
        candidate = candidate.strip().split(' ')[0] + '+' + candidate.strip().split(' ')[-1]
    print candidate
    state = '+'.join(state.split(' '))
    district_type = '+'.join(district_type.split(' '))
    district_name = '+'.join(district_name.strip().split(' '))
    search_urls = []
    #search_urls.append('https://www.googleapis.com/customsearch/v1?cx=011743744063680272768:2oildgpr9n0&key=AIzaSyCdHlGJuMzGBH9hNsEMObffDIkzJ44EQhA&hl=en&q={name}+{state}+2012'.format(name=candidate, state=state, district_type=district_type))
    #search_urls.append(u'https://www.googleapis.com/customsearch/v1?cx=011743744063680272768:2oildgpr9n0&key=AIzaSyCdHlGJuMzGBH9hNsEMObffDIkzJ44EQhA&hl=en&q={name}+{state}+donate+contribute+volunteer'.format(name=candidate, state=state, district_type=district_type))
    search_urls.append(u'https://www.googleapis.com/customsearch/v1?cx=011743744063680272768:2oildgpr9n0&key=AIzaSyCdHlGJuMzGBH9hNsEMObffDIkzJ44EQhA&hl=en&q={name}+{state}'.format(name=candidate, state=state))
    #search_urls.append(u'https://www.googleapis.com/customsearch/v1?cx=011743744063680272768:2oildgpr9n0&key=AIzaSyCdHlGJuMzGBH9hNsEMObffDIkzJ44EQhA&hl=en&q={name}'.format(name=candidate, state=state))
    #search_urls.append(u'https://www.googleapis.com/customsearch/v1?cx=011743744063680272768:2oildgpr9n0&key=AIzaSyCdHlGJuMzGBH9hNsEMObffDIkzJ44EQhA&hl=en&q={name}+{state}+elect'.format(name=candidate, state=state))
    #search_urls.append(u'https://www.googleapis.com/customsearch/v1?cx=011743744063680272768:2oildgpr9n0&key=AIzaSyCdHlGJuMzGBH9hNsEMObffDIkzJ44EQhA&hl=en&q={name}+{state}+official'.format(name=candidate, state=state))
#    search_urls.append(u'https://www.googleapis.com/customsearch/v1?cx=011743744063680272768:2oildgpr9n0&key=AIzaSyCdHlGJuMzGBH9hNsEMObffDIkzJ44EQhA&hl=en&q={name}+{state}+campaign'.format(name=candidate, state=state))
#    search_urls.append(u'https://www.googleapis.com/customsearch/v1?cx=011743744063680272768:2oildgpr9n0&key=AIzaSyCdHlGJuMzGBH9hNsEMObffDIkzJ44EQhA&hl=en&q="{name}+for"+{state}'.format(name=candidate, state=state))
    #search_urls.append(u'https://www.googleapis.com/customsearch/v1?cx=011743744063680272768:2oildgpr9n0&key=AIzaSyCdHlGJuMzGBH9hNsEMObffDIkzJ44EQhA&hl=en&q={name}+{state}+volunteer'.format(name=candidate, state=state, district_type=district_type))
#    search_urls.append(u'https://www.googleapis.com/customsearch/v1?cx=011743744063680272768:2oildgpr9n0&key=AIzaSyCdHlGJuMzGBH9hNsEMObffDIkzJ44EQhA&hl=en&q={name}+{state}+{district_type}'.format(name=candidate, state=state, district_type=district_type))
    #search_urls.append(u'https://www.googleapis.com/customsearch/v1?cx=011743744063680272768:2oildgpr9n0&key=AIzaSyCdHlGJuMzGBH9hNsEMObffDIkzJ44EQhA&hl=en&q={name}+{state}+{district_type}+campaign+2012'.format(name=candidate, state=state, district_type=district_type))
    #search_urls.append(u'https://www.googleapis.com/customsearch/v1?cx=011743744063680272768:2oildgpr9n0&key=AIzaSyCdHlGJuMzGBH9hNsEMObffDIkzJ44EQhA&hl=en&q={name}+{state}+{district_type}+{district_name}+2012'.format(name=candidate, state=state, district_type=district_type, district_name=district_name))
    #search_urls.append(u'https://www.googleapis.com/customsearch/v1?cx=011743744063680272768:2oildgpr9n0&key=AIzaSyCdHlGJuMzGBH9hNsEMObffDIkzJ44EQhA&hl=en&q={name}+{state}+{district_type}+{district_name}+campaign+2012'.format(name=candidate, state=state, district_type=district_type, district_name=district_name))
    #print search_urls
    search_urls = [s.encode(chardet.detect(s.encode('utf-8'))['encoding']) for s in search_urls]
    old_webpage = webpage
    try:
        if not re.match(r'https?://.+', webpage):
            webpage = 'http://'+webpage
        webpage = ul.urlopen(webpage, timeout=10).geturl()
    except:
        pass
    if webpage.lower() != old_webpage.lower() and webpage.lower() != 'http://'+old_webpage.lower():
        print 'webpage change'
        print webpage + '\t' + old_webpage
    websites = []
    webpage_stripped = re.match(r'(?:http://)?(?:www\.)?(?P<content>.+)',webpage).groupdict()['content'].rstrip('/')
    old_webpage_stripped = re.match(r'(?:http://)?(?:www\.)?(?P<content>.+)',old_webpage).groupdict()['content'].rstrip('/')
    #webpage_stripped = webpage.lstrip('http://').lstrip('www.').rstrip('/')
    patt = re.compile(r'^https?://(?:www\.)?{webpage}/?$'.format(webpage=webpage_stripped.lower()))
    old_patt = re.compile(r'^https?://(?:www\.)?{webpage}/?$'.format(webpage=old_webpage_stripped.lower()))
    n = 1
    while True:
        try:
            results = map(lambda x: json.loads(ul.urlopen(x).read()),search_urls)
            break
        except ul.HTTPError as error:
            if error.getcode() == 403 or error.getcode() == 503:
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
    search_text = [[u'{title} {link} {pagemap} {snippet}'.format(**convert_pagemap_dict(i)).lower().encode('utf-8') for i in r['items']] for r in results]
    #search_text = [[u'{title} {link} {pagemap} {snippet}'.format(**i).lower().encode('utf-8') for i in r['items']] for r in results]
    search_class = [map(lambda s: patt.match(s) != None,sl) or map(lambda s: old_patt.match(s) != None,sl) for sl in search_links]
    #print search_text
    ssv = [any(map(patt.match,sl)) or any(map(old_patt.match,sl)) for sl in search_links]
    non_websites = [[i['link'] for i in r['items'] if webpage not in i['link']] for r in results]
    return non_websites, ssv, webpage_stripped, search_links, search_text, search_class

youtube_accept = ['title','description']
def extract_pagemap_text(pagemap, text= '', youtube=False):
    if type(pagemap) == list:
        for item in pagemap:
            text = extract_pagemap_text(item, text, youtube)
        return text
    elif type(pagemap) == dict:
        for k,v in pagemap.iteritems():
            if youtube and k not in youtube_accept:
                continue
            if k == 'videoobject':
                text = extract_pagemap_text(v, text, True)
            elif k == 'metatags' and pagemap.has_key('videoobject'):
                text = extract_pagemap_text(v, text, True)
            else:
                text = extract_pagemap_text(v, text, youtube)
        return text
    elif type(pagemap) == str or type(pagemap) == unicode:
        text += ' ' + pagemap
        return text

def convert_pagemap_dict(item):
    d = {}
    d.update(item)
    if d.has_key('pagemap'):
        d['pagemap'] = extract_pagemap_text(d['pagemap'])
    else:
        d['pagemap'] = ''
    #if not d.has_key('pagemap'):
    #    d['pagemap'] = ''
    return d

lock = Lock()
def runit(l, uid):
    print l['candidate_url']
    try:
        non_webpage_list, search_success_vector, webpage,sl,st,sc = getlinks(l['name'].decode('utf-8').strip(), l['candidate_url'].decode('utf-8').strip(), l['state'].decode('utf-8').strip(), l['electoral_district_type'].decode('utf-8').strip(), l['electoral_district_name'].decode('utf-8').strip())
    except Exception as error:
        import traceback; print traceback.format_exc()
        print error
        return uid,[],[],[],[],[]
    return uid,non_webpage_list, search_success_vector, webpage,sl,st,sc


if __name__ == '__main__':
    with open('ex/webpages_ex.csv') as f, open('non/nonwebpages.csv','w') as g, open('non/webpage_ssv.csv','w') as h, open('search_results.csv','w') as k:
        csvr = csv.DictReader(f)
        csvw = csv.writer(g)
        csvw2 = csv.writer(h)
        csvw3 = csv.writer(k)
        csvw.writerow(['uid','webpage','non_webpage_list'])
        csvw2.writerow(['uid','webpage','search_success_vector'])
        csvw3.writerow(['uid','link','text','class'])
        search_rows_written = [0]
        pool = Pool(processes=10)
        def callb(results):
            uid = results[0]
            nwl = results[1]
            ssv = results[2]
            webpage = results[3]
            sl = results[4]
            st = results[5]
            sc = results[6]
            lock.acquire()
            csvw.writerow([uid,webpage,nwl])
            csvw2.writerow([uid,webpage,ssv])
            global csvw3
            rotate = False
            for i in range(len(sl)):
                for j in range(len(sl[i])):
                    csvw3.writerow([uid,sl[i][j],st[i][j],sc[i][j]])
                    search_rows_written[0] += 1
                    if search_rows_written[0] % 1000 == 0:
                        rotate=True
            if rotate:
                #csvw3 = csv.writer(open('search_results{num}.csv'.format(num=search_rows_written[0]/1000),'w'))
                pass
            lock.release()
        for l in csvr:
            uid = l['identifier']
            pool.apply_async(runit, [l,uid], callback=callb)
        pool.close()
        pool.join()

DEFAULT_TABLE = {
        'skip_head_lines':0,
        'format':'csv',
        'field_sep':',',
        'quotechar':'"',
        'copy_every':25,
        'udcs':{
            },
        }
SITES_PLAIN = dict(DEFAULT_TABLE)
SITES_PLAIN.update({
    'table':'pages_for_class',
    'filename':'/home/gaertner/code/candclass/search_results.csv',
    'field_sep':',',
    'columns':{
        'uid':1,
        'class':4,
        ('sitetext','website'):{'function':get_page_for_class,'columns':(2,)},
        },
    })
ERSATZPG_CONFIG.update({
    'use_utf':False,
    'tables':{
        #'sites_ajax':SITES_AJAX,
        'pages_for_class':SITES_PLAIN,
        },
    #'parallel_load':({'tables':('sites_ajax','sites_plain'),'keys':{}},),
    'parallel_load':(),
    'key_sources':{},
    })
