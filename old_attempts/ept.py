import urllib2 as ul
candidate = 'mark+scheffel'
state = 'colorado'
url = u'https://www.googleapis.com/customsearch/v1?cx=011743744063680272768:2oildgpr9n0&key=AIzaSyCdHlGJuMzGBH9hNsEMObffDIkzJ44EQhA&hl=en&q={name}+{state}'.format(name=candidate, state=state)
site = ul.urlopen(url)
import json
j = json.loads(site.read())
r = j['items']
youtube_accept = ['title','description']
def extract_pagemap_text(pagemap, text= [], youtube=False):
    if type(pagemap) == list:
        for l in pagemap:
            text = extract_pagemap_text(l, text, youtube)
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
        text.append(pagemap)
        return text

def convert_pagemap_dict(item):
    d = {}
    d.update(item)
    if d.has_key('pagemap'):
        d['pagemap'] = ' '.join(extract_pagemap_text(d['pagemap']))
    else:
        d['pagemap'] = ''
    return d

print '{title} {displayLink} {pagemap} {snippet}'.format(**convert_pagemap_dict(r[1]))
