import chardet
import conversions
import csv
import json
import random
import re
import requests
import sys
import time
import urllib2 as ul

from multiprocessing import Pool, Lock

from state_map import state_map


def getlinks(candidate, webpage, state, district_type, district_name):

    district_type = district_type.replace('_', ' ').strip()
    district_type = '+'.join(district_type.split(' '))
    district_name = '+'.join(district_name.strip().split(' '))

    state = state_map[state.strip()]
    state = '+'.join(state.split(' '))

    candidate, last, first = conversions.clean_name(candidate)
    candidate = '+'.join(candidate.split(' '))
    #print candidate

    search_urls = []
    precise_searches = []

    url = 'https://www.googleapis.com/customsearch/v1'
    cx = '011743744063680272768:2oildgpr9n0'
    key = 'AIzaSyCdHlGJuMzGBH9hNsEMObffDIkzJ44EQhA'

    search_urls.append(
        u'{url}?cx={cx}&key={key}&hl=en&q={name}+{state}'.format(
            url=url, cx=cx, key=key, name=candidate, state=state)
    )

    precise_searches.append(
        u'{url}?cx={cx}&key={key}&hl=en&q={name}+{state}+campaign'.format(
            url=url, cx=cx, key=key, name=candidate, state=state)
    )

    precise_searches.append(
        u'{url}?cx= {cx}&key={key}&hl=en&q={name}+{state}+elect'.format(
            url=url, cx=cx, key=key, name=candidate, state=state)
    )

    search_urls = [
        s.encode(chardet.detect(s.encode('utf-8'))['encoding'])
        for s in search_urls
    ]

    precise_searches = [s.encode(chardet.detect(s.encode('utf-8'))['encoding']) for s in precise_searches]
    
    #print 'SEARCH_URLS: {}'.format(search_urls)
    old_webpage = webpage
    if webpage != 'www.gernensamples.com':
        webpage = conversions.get_redirect(webpage)

    webpage_stripped = re.match(
        r'(?:https?://)?(?:www\.)?(?P<content>.+)', webpage
    ).groupdict()['content'].rstrip('/')

    old_webpage_stripped = re.match(
        r'(?:https?://)?(?:www\.)?(?P<content>.+)', old_webpage
    ).groupdict()['content'].rstrip('/')

    # TODO strip queries
    webpage_no_queries = ul.urlparse.urlparse(webpage)
    webpage_no_queries = re.match(
        r'(?:www\.)?(?P<content>.+)',
        webpage_no_queries.netloc + webpage_no_queries.path
    ).groupdict()['content'].rstrip('/')
    old_webpage_no_queries = ul.urlparse.urlparse(old_webpage)
    old_webpage_no_queries = re.match(
        r'(?:www\.)?(?P<content>.+)',
        old_webpage_no_queries.netloc + old_webpage_no_queries.path
    ).groupdict()['content'].rstrip('/')

    patt = re.compile(r'^https?://(?:www.)?{webpage}/?$'.format(
        webpage=webpage_stripped.lower())
    )
    old_patt = re.compile(r'^https?://(?:www.)?{webpage}/?$'.format(
        webpage=old_webpage_stripped.lower())
    )

    n = 4
    while True:
        results = map(
            lambda x: json.loads(requests.get(x).text), search_urls
        )
        if any(map(
                lambda r: 'error' in r and (r['error']['code'] == 403
                                            or r['error']['code'] == 503),
                results)):
            print 'sleeping'
            time.sleep(n + random.randint(1, 1000)/1000.)
            n = n*2
        elif any(map(lambda r: 'error' in r, results)):
            raise Exception(', '.join(
                map(lambda r: r['error']['message'],
                    filter(lambda r: 'error' in r, results)))
            )
        else:
            break

    n = 4
    while True:
        precise_results = map(
            lambda x: json.loads(requests.get(x).text), precise_searches
        )
        if any(map(
            lambda r: 'error' in r and (r['error']['code'] == 403 or r['error']['code'] == 503),precise_results)):
            print 'sleeping'
            time.sleep(n + random.randint(1,1000)/1000.)
            n = n*2
        elif any(map(lambda r: r.has_key('error'), precise_results)):
            raise Exception(', '.join(map(lambda r: r['error']['message'], filter(lambda r: 'error' in r, precise_results))))
        else:
            break

#    print 'RESULTS:{}'.format(results)
    if type(results) != list:
        results = [results]

    real_results = [(r if 'items' in r else {'items': []}) for r in results]
    results = real_results

    search_links = [
        [
            i['link'].lower() for i in r['items']
        ] for r in results
    ]

    search_text = [
        [
            u'{title} {link} {pagemap} {snippet}'.format(
                **convert_pagemap_dict(i)
            ).lower().encode('utf-8') for i in r['items']
        ] for r in results
    ]

   # print 'SL:{}'.format(search_links)
    for ri in range(len(search_links)):
        for si in range(len(search_links[ri])):
            for r in precise_results:
                if r.has_key('items'):
                    for i in r['items']:
                        if conversions.child_or_equal_page(search_links[ri][si], i['link'].lower(), True):
                            search_text[ri][si] += ' bipspecialappearsinprecise'
    child_links = []
    child_text = []
    search_class = [map(lambda s: conversions.page_relation(s, True, webpage, old_webpage), sl) for sl in search_links]

    #TODO Clean up ssv code
    ssv = [any(map(patt.match,sl)) or any(map(old_patt.match,sl)) for sl in search_links]
    non_websites = [[i['link'] for i in r['items'] if webpage not in i['link']] for r in results]
    cs,ct,cc = zip(*[combine_children(search_links[i], search_text[i], search_class[i], child_links, child_text) for i in range(len(search_links))])
    print 'got there',len(results[0]['items'])

    return non_websites, ssv, webpage_stripped, search_links, search_text, [r['items'] for r in results], search_class, cs, ct, cc,child_links,child_text

classes= ('ParentCombined','TrueCombined','ChildCombined','FalseCombined', 'Parent','True','Child','False')
class_ranks = dict((classes[i],i) for i in range(len(classes)))

def class_order(cls1,cls2):
    return class_ranks[cls2]-class_ranks[cls1]

def combine_children(websites, texts, classes, child_links, child_text):
    combined_sites = {'websites':[],'texts':[],'classes':[]}

    sites_classes = zip(websites, classes)
    for site, cls in sites_classes:
        group = filter(lambda s: class_order(conversions.page_relation(s[0], True, site), 'False') > 0,zip(websites,classes,texts))
        try:
            min_tuple = min(group,key=lambda g:class_ranks[g[1]])
        except:
            import pdb;pdb.set_trace()
        group_class = min_tuple[1]+'Combined'
        group_site = min_tuple[0]
        group_children_text = []
        child_site_texts = zip(child_links, child_text)
        for child,ctext in child_site_texts:
            if conversions.child_or_equal_page(group_site, child, True):
                group_children_text.append(ctext)
                child_idx=child_links.index(child)
                child_links.pop(child_idx)
                child_text.pop(child_idx)
                child_site_texts.pop(child_idx)
        combined_sites['websites'].append(group_site)
        combined_sites['classes'].append(group_class)
        group_text = ' '.join(group_children_text)
        for g in group:
            site_idx = websites.index(g[0])
            websites.pop(site_idx)
            classes.pop(site_idx)
            sites_classes.pop(site_idx)
            texts.pop(site_idx)
        group_text += ' ' + ' '.join(map(lambda g:g[2],group))
        combined_sites['texts'].append(group_text)

    return combined_sites['websites'], combined_sites['texts'],combined_sites['classes']


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
        if not re.match(r'https?://.+', pagemap):
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
    #print 'ok'
    #print l['Website']
    try:
        non_webpage_list, search_success_vector, webpage,sl,st,items,sc,cs,ct,cc,child_links, child_text = getlinks(
            l['Candidate Name'].decode('utf-8').strip(),
            l['Website'].decode('utf-8').strip(),
            l['State'].decode('utf-8').strip(),
            l['type'].decode('utf-8').strip(),
            l['name'].decode('utf-8').strip()
        )
        print 'UID:{}\nNON_WEBPAGE:{}\nSL:{}\nST:{}\nITEMS:{}\nSC:{}\nCS:{}\nCT:{}\nCC:{}\nCHILD LINKS:{}\nCHILD TEXT:{}\n\n'.format(uid,len(non_webpage_list[0]),len(sl[0]),len(st[0]),len(items[0]),len(sc[0]),len(cs[0]),len(ct[0]),len(cc[0]),len(child_links),len(child_text))
    except Exception as error:
        import traceback; print traceback.format_exc()
        print error
        return uid,[],[],[],[],[],[],[],[],[],[],[],[]
    return uid,non_webpage_list, search_success_vector, webpage,sl,st,items,sc,cs,ct,cc, child_links, child_text


if __name__ == '__main__':
    if '--full' in sys.argv:
        full = 'full'
    else:
        full = ''
    filename = sys.argv[1]

    with open('web/{filename}'.format(filename=filename), 'rU') as f, open('web/non/{full}webnonwebpages.csv'.format(full=full),'a') as g, open('web/non/{full}webwebpage_ssv.csv'.format(full=full),'a') as h, open('web/{full}websearch_results.csv'.format(full=full),'a') as k, open('web/{full}websearch_results_combined.csv'.format(full=full),'a') as m:
        csvr = csv.DictReader(f)
        csvw = csv.writer(g)
        csvw2 = csv.writer(h)
        csvw3 = csv.writer(k)
        csvw4 = csv.writer(m)
        # Should uncomment first run.... TODO better system 
#        csvw.writerow(['uid','webpage','non_webpage_list'])
#        csvw2.writerow(['uid','webpage','search_success_vector'])
#        csvw3.writerow(['uid','link','class', 'sitetext','items'])
#        csvw4.writerow(['uid','link','class','sitetext'])
        search_rows_written = [0]
        pool = Pool(processes=20)
        def callb(results):
            #print 'callb'
            uid,nwl,ssv,webpage,sl,st,items,sc,cs,ct,cc,child_links,child_text = results
            #uid = results[0]
            #nwl = results[1]
            #ssv = results[2]
            #webpage = results[3]
            #sl = results[4]
            #sc = results[5]
            lock.acquire()
            csvw.writerow([uid,webpage,nwl])
            csvw2.writerow([uid,webpage,ssv])
            global csvw3
            rotate = False
            for i in range(len(sl)):
                for j in range(len(sl[i])):
                    csvw3.writerow([uid,sl[i][j],sc[i][j],st[i][j],repr(items[i][j])])
                    search_rows_written[0] += 1
                    if search_rows_written[0] % 1000 == 0:
                        rotate=True
            for i in range(len(child_links)):
                csvw3.writerow([uid,child_links[i],'Info',child_text[i],''])
            if rotate:
                #csvw3 = csv.writer(open('search_results{num}.csv'.format(num=search_rows_written[0]/1000),'w'))
                pass
            search_rows_written[0] = 0
            for i in range(len(cs)):
                for j in range(len(cs[i])):
                    csvw4.writerow([uid,cs[i][j],cc[i][j],ct[i][j]])
                    search_rows_written[0] += 1
                    if search_rows_written[0] % 1000 == 0:
                        rotate=True
            if rotate:
                #csvw3 = csv.writer(open('search_results{num}.csv'.format(num=search_rows_written[0]/1000),'w'))
                pass
            lock.release()
        for l in csvr:
            uid = l['UID']
            #callb(runit(l,uid))
            pool.apply_async(runit, [l,uid], callback=callb)
        pool.close()
        pool.join()
