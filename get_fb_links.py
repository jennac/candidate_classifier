import urllib2 as ul
import re
import csv
import json
import time
import random
import sys
import chardet
import conversions
import requests

from state_map import state_map
from multiprocessing import Pool, Lock


def getlinks(candidate, webpage, state, district_type, district_name):
    """
    Gets all the facebook links found via the Google Search API
    """

    # ### Cleanup input variables

    # District
    district_type = district_type.replace('_', ' ').strip()
    district_type = '+'.join(district_type.split(' '))
    district_name = '+'.join(district_name.strip().split(' '))

    # State
    state = state_map[state.strip()]
    state = '+'.join(state.split(' '))

    # Candidate name
    candidate, last, first = conversions.clean_name(candidate)
    candidate = '+'.join(candidate.split(' '))
    #print 'CANDIDATE: {}'.format(candidate)

    # Setup search urls
    search_urls = []
    extra_children_searches = []
    precise_searches = []

    # Common values
    url = "https://www.googleapis.com/customsearch/v1"
    cx = "011743744063680272768:cp4-iesopjm"
    key = "AIzaSyCdHlGJuMzGBH9hNsEMObffDIkzJ44EQhA"

    search_urls.append(
        u'{url}?cx={cx}&key={key}&hl=en&q={name}+{state}'.format(
            url=url, cx=cx, key=key, name=candidate, state=state
        )
    )

    # Just searches for general about pages
    extra_children_searches.append(
        u'{url}?cx={cx}&key={key}&hl=en&q={name}+{state}+info'.format(
            url=url, cx=cx, key=key, name=candidate, state=state
        )
    )

    # sk=info specifies Facebook's about page
    extra_children_searches.append(
        u'{url}?cx={cx}&key={key}&hl=en&q={name}+{state}+sk=info'.format(
            url=url, cx=cx, key=key, name=candidate, state=state
        )
    )

    precise_searches.append(
        u'{url}?cx={cx}&key={key}&hl=en&q={name}+{state}+campaign'.format(
            url=url, cx=cx, key=key, name=candidate, state=state
        )
    )

    precise_searches.append(
        u'{url}?cx={cx}&key={key}&hl=en&q={name}+{state}+elect'.format(
            url=url, cx=cx, key=key, name=candidate, state=state
        )
    )

    # Clean up encoding of URL's
    search_urls = [
        s.encode(
            chardet.detect(s.encode('utf-8'))['encoding']
        ) for s in search_urls
    ]

    extra_children_searches = [
        s.encode(
            chardet.detect(s.encode('utf-8'))['encoding']
        ) for s in extra_children_searches
    ]

    #print 'SEARCH_URLS: {}'.format(search_urls)

    precise_searches = [
        s.encode(
            chardet.detect(s.encode('utf-8'))['encoding']
        ) for s in precise_searches
    ]

    # This must be  a test for a dummy webside used for testing
    # get_redirect simply gets the final page that returns a 200
    old_webpage = webpage
    if webpage != 'www.gernensamples.com':
        webpage = conversions.get_redirect(webpage)

    #print 'WBBPAGES: {}'.format(webpage)

    has_webpage = True
    #    raise Exception  # why do we need this exception??
    # print 'ok?'
    # Cleanup web pages by removing protocol, subdomain, and trailing '/'

    if has_webpage:
        #print has_webpage
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
        
        #print 'NO:{}'.format(old_webpage_no_queries)
        if old_webpage_no_queries is not None:
            old_webpage_no_queries = re.match(
                r'(?:www\.)?(?P<content>.+)',
                old_webpage_no_queries.netloc + old_webpage_no_queries.path
            ).groupdict()['content'].rstrip('/')

        patt = re.compile(
            r'^https?://(?:www.)?{webpage}/?$'.format(
                webpage=webpage_stripped.lower()
            )
        )
        old_patt = re.compile(
            r'^https?://(?:www.)?{webpage}/?$'.format(
                webpage=old_webpage_stripped.lower()
            )
        )

        child_patt = re.compile(
            r'^https?://(?:www\.)?{webpage}.+'.format(
                webpage=webpage_no_queries.lower()
            )
        )

        old_child_patt = re.compile(
            r'^https?://(?:www\.)?{webpage}.+'.format(
                webpage=old_webpage_no_queries.lower()
            )
        )

    print 'starting'
    n = 4
    while True:
        results = map(lambda x: json.loads(requests.get(x).text), search_urls)
        #for r in results:
        #    print 'error' in r
        if any(map(
                lambda r: ('error' in r and (
                    r['error']['code'] == 403 or r['error']['code'] == 503)
                ), results)):
            print 'sleeping'
            time.sleep(n + random.randint(1, 1000)/1000.)
            n = n*2
        elif any(map(lambda r: 'error' in r, results)):
            raise Exception(', '.join(
                map(
                    lambda r: r['error']['message'],
                    filter(lambda r: 'error' in r, results)
                )
            ))
        else:
            break

    n = 4
    while True:
        child_results = map(
            lambda x: json.loads(requests.get(x).text),
            extra_children_searches
        )
        if any(map(
                lambda r: 'error' in r and (
                    r['error']['code'] == 403 or r['error']['code'] == 503
                ), child_results)):
            print 'sleeping'
            time.sleep(n + random.randint(1, 1000) / 1000.)
            n = n * 2
        elif any(map(
            lambda r: 'error' in r, child_results
        )):
            raise Exception(', '.join(
                map(
                    lambda r: r['error']['message'],
                    filter(lambda r: 'error' in r, child_results)
                )
            ))
        else:
            break

    n = 4
    while True:
        precise_results = map(
            lambda x: json.loads(requests.get(x).text), precise_searches
        )
        if any(map(
            lambda r: 'error' in r and (
                r['error']['code'] == 403 or r['error']['code'] == 503
                ), precise_results)):
            print 'sleeping'
            time.sleep(n + random.randint(1, 1000) / 1000.)
            n = n * 2
        elif any(map(lambda r: 'error' in r, precise_results)):
            raise Exception(', '.join(
                map(
                    lambda r: r['error']['message'],
                    filter(lambda r: 'error' in r, precise_results)
                )
            ))
        else:
            break

  
    if type(results) != list:
        results = [results]

    # Get results from the "items" key and store it in the results variable
    real_results = [
        (r if 'items' in r else {'items': []}) for r in results
    ]
    results = real_results

    # print 'RESULTS:{}'.format(results)
    # Get the result URLs, Extract searchable text from the pagemap
    search_links = [[i['link'].lower() for i in r['items']] for r in results]
    search_text = [
        [u'{title} {link} {pagemap} {snippet}'.format(
            **convert_pagemap_dict(i)
        ).lower().encode('utf-8') for i in r['items']] for r in results
    ]

    # first loop may be unneccessary
    for ri in range(len(search_links)):  # for 1 to number of result objects
        for si in range(len(search_links[ri])):  # for 1 to number of links
            # For each "precise result" (name+state+'elect'),
            # see if the link is equivalent
            # or a sub page of the main results (name+state)
            for r in precise_results:
                if 'items' in r:
                    for i in r['items']:
                        if conversions.child_or_equal_page(
                            search_links[ri][si], i['link'].lower(), True
                        ):
                            search_text[ri][si] += ' bipspecialappearsinprecise'  # noqa

    # Get the result URLs, Extract searchable text from the pagemap
    child_links = [
        i['link'].lower() for r in child_results if 'items' in r
        for i in r['items']
    ]

    child_text = [
        u'{title} {link} {pagemap} {snippet}'.format(
            **convert_pagemap_dict(i)
        ).lower().encode('utf-8') for r in child_results if 'items' in r
        for i in r['items']
    ]

    # Classify each search link based on it's relationship
    # to the provided web page, either PARENT, CHILD, TRUE (identity),
    # or FALSE (no match)
    search_class = [
        map(lambda s: conversions.page_relation(
            s, True, webpage, old_webpage
        ), sl) for sl in search_links
    ]

    # TODO Clean up ssv code

    # Seems to match each search link result against the webpage domain
    ssv = [
        any(map(patt.match, sl)) or any(map(old_patt.match, sl))
        for sl in search_links
    ]

    non_websites = [
        [i['link'] for i in r['items'] if webpage not in i['link']]
        for r in results
    ]

    cs, ct, cc = zip(
        *[combine_children(
            search_links[i], search_text[i], search_class[i],
            child_links, child_text
        ) for i in range(len(search_links))]
    )

    print 'got there', len(results[0]['items'])

    return (non_websites, ssv, webpage_stripped, search_links, search_text,
            [r['items'] for r in results], search_class, cs, ct, cc,
            child_links, child_text)

# CONSTANTS
classes = ('ParentCombined', 'TrueCombined', 'ChildCombined', 'FalseCombined',
           'Parent', 'True', 'Child', 'False')

class_ranks = dict((classes[i], i) for i in range(len(classes)))
youtube_accept = ['title', 'description']

# HELPER METHODS


def class_order(cls1, cls2):
    """
    Determines the order distance between two classes
    """
    return class_ranks[cls2]-class_ranks[cls1]


def combine_children(websites, texts, classes, child_links, child_text):
    """
    Combines child sites into a group under the main parent site,
    and returns a tuple consisting of the parent site, the parent class,
    and the combined text from all child sites.
    """
    combined_sites = {'websites': [], 'texts': [], 'classes': []}
    # root_sites = []
    # temp_root_sites = {}

    # Create tuples of each search link with its class
    # (i.e. parent, child, identical, no-match)
    sites_classes = zip(websites, classes)

    # Loop over each site and combine
    for site, cls in sites_classes:

        # This creates a group object that contains
        # a list of tuples for every search result
        # which is a child, parent, or equal of another search result
        # for this candidate
        group = filter(
            lambda s: class_order(
                conversions.page_relation(s[0], True, site), 'False'
            ) > 0,
            zip(websites, classes, texts)
        )

        # Get the lowest ranked classes, which at this point should be
        # parents
        try:
            min_tuple = min(group, key=lambda g: class_ranks[g[1]])
        except:
            import pdb
            pdb.set_trace()

        # Setup the '{class}combined' group
        group_class = min_tuple[1]+'Combined'
        group_site = min_tuple[0]
        group_children_text = []

        # Create a tuple of child pages with their text
        child_site_texts = zip(child_links, child_text)

        # Loop over each child site and determine if it is
        # a child of the group_site website.  If so, append its info
        # to the group_children_text list and remove it from the list
        # of child sites
        for child, ctext in child_site_texts:
            if conversions.child_or_equal_page(group_site, child, True):
                group_children_text.append(ctext)
                child_idx = child_links.index(child)
                child_links.pop(child_idx)
                child_text.pop(child_idx)
                child_site_texts.pop(child_idx)

        # Add this site to the combined_sites dict
        combined_sites['websites'].append(group_site)
        combined_sites['classes'].append(group_class)
        group_text = ' '.join(group_children_text)

        # Remove each site in this group from the various lists
        for g in group:
            site_idx = websites.index(g[0])
            websites.pop(site_idx)
            classes.pop(site_idx)
            sites_classes.pop(site_idx)
            texts.pop(site_idx)

        # Add the group's text to to the combined_sites dict
        group_text += ' ' + ' '.join(map(lambda g: g[2], group))
        combined_sites['texts'].append(group_text)

    return (combined_sites['websites'], combined_sites['texts'],
            combined_sites['classes'])


def extract_pagemap_text(pagemap, text='', youtube=False):
    """
    Extracts useable text for searching from
    Google's Pagemap object
    """

    if type(pagemap) == list:
        for item in pagemap:
            text = extract_pagemap_text(item, text, youtube)
        return text

    elif type(pagemap) == dict:
        for k, v in pagemap.iteritems():
            if youtube and k not in youtube_accept:
                continue
            if k == 'videoobject':
                text = extract_pagemap_text(v, text, True)
            elif k == 'metatags' and 'videoobject' in pagemap:
                text = extract_pagemap_text(v, text, True)
            else:
                text = extract_pagemap_text(v, text, youtube)
        return text
    elif type(pagemap) == str or type(pagemap) == unicode:
        if not re.match(r'https?://.+', pagemap):
            text += ' ' + pagemap
        return text


def convert_pagemap_dict(item):
    """
    Converts and item's pagemap to just the text
    """
    d = {}
    d.update(item)
    if 'pagemap' in d:
        d['pagemap'] = extract_pagemap_text(d['pagemap'])
    else:
        d['pagemap'] = ''
    return d

lock = Lock()


def runit(l, uid):
    #print '---------START----------------'
    #print l.keys()
    #print 'FACEBOOK: {}'.format(l['Facebook URL'])
    #print l['name']
    try:
        non_webpage_list, search_success_vector, webpage, sl, st, items, sc, cs, ct, cc, child_links, child_text = getlinks(
                l['Candidate Name'].decode('utf-8').strip(),
                l['Facebook URL'].decode('utf-8').strip(),
                l['State'].decode('utf-8').strip(),
                l['type'].decode('utf-8').strip(),
                l['name'].decode('utf-8').strip()
            )
        print 'UID:{}\nNON_WEBPAGE:{}\nSL:{}\nST:{}\nITEMS:{}\nSC:{}\nCS:{}\nCT:{}\nCC:{}\nCHILD LINKS:{}\nCHILD TEXT:{}\n\n'.format(uid,len(non_webpage_list[0]),len(sl[0]),len(st[0]),len(items[0]),len(sc[0]),len(cs[0]),len(ct[0]),len(cc[0]),len(child_links),len(child_text))
#        print uid, len(non_webpage_list[0]), len(sl[0]), len(st[0]), \
#            len(items[0]), len(sc[0]), len(cs[0]), len(ct[0]), len(cc[0]), \
#            len(child_links), len(child_text)
    except Exception as error:
        import traceback
        print traceback.format_exc()
        print error
        #print '-------ENDEXCEP------------------'
        return uid, [], [], [], [], [], [], [], [], [], [], [], []
    #print '---------END----------------'
    return (uid, non_webpage_list, search_success_vector, webpage,
            sl, st, items, sc, cs, ct, cc, child_links, child_text)


if __name__ == '__main__':
    if '--full' in sys.argv:
        full = 'full'
    else:
        full = ''
    filename = sys.argv[1]
    with open('fb/{filename}'.format(filename=filename), 'rU') as f,\
        open('fb/non/{full}fbnonwebpages.csv'.format(full=full), 'a') as g,\
        open('fb/non/{full}fbwebpage_ssv.csv'.format(full=full), 'a') as h,\
        open('fb/{full}fbsearch_results.csv'.format(full=full), 'a') as k,\
        open('fb/{full}fbsearch_results_combined.csv'.format(full=full), 'a') as m:  # noqa

        csvr = csv.DictReader(f)
        csvw = csv.writer(g)
        csvw2 = csv.writer(h)
        csvw3 = csv.writer(k)
        csvw4 = csv.writer(m)
        #csvw.writerow(['uid', 'webpage', 'non_webpage_list'])
        #csvw2.writerow(['uid', 'webpage', 'search_success_vector'])
        #csvw3.writerow(['uid', 'link', 'class', 'sitetext', 'items'])
        #csvw4.writerow(['uid', 'link', 'class', 'sitetext'])
        search_rows_written = [0]
        pool = Pool(processes=20)

        def callb(results):
            (uid, nwl, ssv, webpage, sl, st, items, sc, cs,
                ct, cc, child_links, child_text) = results
            lock.acquire()
            csvw.writerow([uid, webpage, nwl])
            csvw2.writerow([uid, webpage, ssv])
            global csvw3
            rotate = False
            for i in range(len(sl)):
                for j in range(len(sl[i])):
                    csvw3.writerow(
                        [uid, sl[i][j], sc[i][j], st[i][j], repr(items[i][j])]
                    )
                    search_rows_written[0] += 1
                    if search_rows_written[0] % 1000 == 0:
                        rotate = True
            for i in range(len(child_links)):
                csvw3.writerow(
                    [uid, child_links[i], 'Info', child_text[i], '']
                )
            if rotate:
                # csvw3 = csv.writer(
                #    open('search_results{num}.csv'.format(
                #        num=search_rows_written[0]/1000),'w'))
                pass

            search_rows_written[0] = 0

            for i in range(len(cs)):
                for j in range(len(cs[i])):
                    csvw4.writerow([uid, cs[i][j], cc[i][j], ct[i][j]])
                    search_rows_written[0] += 1
                    if search_rows_written[0] % 1000 == 0:
                        rotate = True
            if rotate:
                # csvw3 = csv.writer(
                #    open('search_results{num}.csv'.format(
                #        num=search_rows_written[0]/1000),'w'))
                pass

            lock.release()

        for l in csvr:
            uid = l['UID']
            pool.apply_async(runit, [l, uid], callback=callb)

        pool.close()
        pool.join()
