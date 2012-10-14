import urllib2 as ul
import sys
q = '+'.join(sys.argv[1:-1])
url = u'https://www.googleapis.com/customsearch/v1?cx=011743744063680272768:2oildgpr9n0&key=AIzaSyCdHlGJuMzGBH9hNsEMObffDIkzJ44EQhA&hl=en&q={q}'.format(q=q)
site = ul.urlopen(url)
import json
j = json.loads(site.read())
r = j['items']
youtube_accept = ['title','description']
def extract_pagemap_text(pagemap, text='', youtube=False):
    #print pagemap
    #lock.acquire()
    if type(pagemap) == list:
        for l in pagemap:
            text = extract_pagemap_text(l, text, youtube)
        #lock.release()
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
        #lock.release()
        return text
    elif type(pagemap) == str or type(pagemap) == unicode:
        text += ' ' + pagemap
        del pagemap
        pass
        #lock.release()
        return text
from multiprocessing import Lock
lock = Lock()
def convert_pagemap_dict(item):
    #lock.acquire()
    print 'converting'
    d = {}
    d.update(item)
    if d.has_key('pagemap'):
        print 'extracting pagemap'
        e = extract_pagemap_text(d['pagemap'])
        d['pagemap'] = e
        del e
        print 'pagemap extracted'
    else:
        d['pagemap'] = ''
    #lock.release()
    return d
d1 = r[int(sys.argv[-1])]
q = 'jim+pirtle+colorado'
url = u'https://www.googleapis.com/customsearch/v1?cx=011743744063680272768:2oildgpr9n0&key=AIzaSyCdHlGJuMzGBH9hNsEMObffDIkzJ44EQhA&hl=en&q={q}'.format(q=q)
site = ul.urlopen(url)
import json
j = json.loads(site.read())
r2 = j['items']
d2 = r2[int(sys.argv[-1])]
print d1
print '='*100
print d2
def runall(r):
    search_text = []
    for i in r:
        d = convert_pagemap_dict(i)
        search_text.append(u'{title} {link} {pagemap} {snippet}'.format(**d))
        del d
    #print search_text
from multiprocessing import Pool
pool = Pool(processes =1)
def callb(d):
    print '='*100
    print u'{title} {displayLink} {pagemap} {snippet}'.format(**d)
#pool.apply_async(runall, [r])
#pool.apply_async(runall, [r2])
for i in range(10):
    pool.apply_async(convert_pagemap_dict, [d1],callback=callb)
    pool.apply_async(convert_pagemap_dict, [d2],callback=callb)
pool.close()
pool.join()
