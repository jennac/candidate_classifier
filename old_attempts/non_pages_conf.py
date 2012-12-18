from univ_settings import ERSATZPG_CONFIG
from collections import OrderedDict

import urllib2 as ul
from urllib2 import URLError
import re, chardet, os
from state_map import state_map
from new_pages_conf import getplain
import random
num_sites = 2
random.seed(1121)
search_results = (0,)
def get_non_sites(webpage, urls_collection):
    urls_collection = eval(urls_collection)
    webpage_stripped = re.match(r'(?:http://)?(?:www\.)?(?P<content>.+)',webpage).groupdict()['content'].rstrip('/')
    patt = re.compile(r'^http://(?:www.)?{webpage}/?$'.format(webpage=webpage_stripped.lower()))
    pages = []
    for n in search_results:
        urls = [u for u in urls_collection[n] if not patt.match(u) and os.path.splitext(u)[1] in ['html','htm','php','']]
        random.shuffle(urls)
        urls = urls[:num_sites]
        for u in urls:
            pages.append(getplain(u))
    pages += ['']*(num_sites-len(pages))
    return tuple(pages)


DEFAULT_TABLE = {
        'skip_head_lines':0,
        'format':'csv',
        'field_sep':',',
        'quotechar':'"',
        'copy_every':100,
        'udcs':{
            },
        }

NON_SITES = dict(DEFAULT_TABLE)
NON_SITES.update({
    'table':'non_sites_plain',
    'filename':'/home/gaertner/code/candclass/nonwebpages.csv',
    'field_sep':',',
    'columns':{
        'uid':1,
        'webpage':2,
        tuple('sitetext{i}'.format(i=i) for i in range(1,num_sites*len(search_results)+1)):{'function':get_non_sites,'columns':(2,3)},
        },
    })

ERSATZPG_CONFIG.update({
    #    'use_utf':True,
    'tables':{
        'non_sites_plain':NON_SITES,
        },
    'parallel_load':(),
    'key_sources':{},
    })

