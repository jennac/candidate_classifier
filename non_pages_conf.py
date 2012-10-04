from univ_settings import ERSATZPG_CONFIG
from collections import OrderedDict

import urllib2 as ul
from urllib2 import URLError
from selenium import webdriver
from selenium.common.exceptions import WebDriverException, TimeoutException
from bs4 import BeautifulSoup
import re, chardet
from state_map import state_map
num_sites = 6

def getlinks(candidate, webpage, state, district_type, district_name):
    district_type = district_type.strip().replace('district','').replace('_',' ')
    state = state_map[state.strip()]
    candidate = '+'.join(candidate.strip().split(' '))
    state = '+'.join(state.split(' '))
    district_type = '+'.join(district_type.split(' '))
    district_name = '+'.join(district_name.strip().split(' '))
    search_url = 'https://www.googleapis.com/customsearch/v1?cx=011743744063680272768:2oildgpr9n0&key=AIzaSyCdHlGJuMzGBH9hNsEMObffDIkzJ44EQhA&hl=en&q={name}+{state}+{district_type}'.format(name=candidate, state=state, district_type=district_type)
    search_url_dist_name = 'https://www.googleapis.com/customsearch/v1?cx=011743744063680272768:2oildgpr9n0&key=AIzaSyCdHlGJuMzGBH9hNsEMObffDIkzJ44EQhA&hl=en&q={name}+{state}+{district_type}+{district_name}'.format(name=candidate, state=state, district_type=district_type, district_name=district_name)
    non_websites = []
    browser.get(search_url)
    searchsite = BeautifulSoup(browser.page_source)
    search_links = [l.get('href') for l in searchsite.find_all('a','l')]
    for l in search_links:
        if webpage not in l:
            non_websites.append(l)
            if len(non_websites) == num_sites/2:
                break
    non_websites_dist_name = []
    browser.get(search_url_dist_name)
    searchsite = BeautifulSoup(browser.page_source)
    search_links = [l.get('href') for l in searchsite.find_all('a','l')]
    for l in search_links:
        if webpage not in l:
            non_websites_dist_name.append(l)
            if len(non_websites_dist_name) == num_sites/2:
                break
    return non_websites + non_websites_dist_name

def getplain(candidate, webpage, state, district_type, district_name):
    non_websites = getlinks(candidate, webpage, state, district_type, district_name)
    non_websites_text = ['no_result']*num_sites
    for i in range(len(non_websites)):
        url = non_websites[i]
        try:
            site = ul.urlopen(url, timeout=15)
            n = site.read()
            soup = BeautifulSoup(n)
            n = soup.get_text()
            try:
                n = n.encode('utf-8')
            except:
                d = chardet.detect(n)
                n = n.decode(d['encoding']).encode('utf-8')

        except URLError as error:
            if (type(error.reason) == str and error.reason == 'timed out'):
                n = 'TIMEDOUT'
            else:
                try:
                    if error.reason.message == 'timed out':
                        n = 'TIMEDOUT'
                    else:
                        n = 'MALFORMED_URI'
                except AttributeError:
                    n = 'MALFORMED_URI'
        except Exception as error:
            if error.message == 'timed out':
                n = 'TIMEDOUT'
            else:
                raise error
        #return n.decode(d['encoding']).encode('utf-8'),
        non_websites_text[i] = n
    return tuple(non_websites_text)

ajax_browsers = []
for i in range(num_sites):
    ajax_browsers.append(webdriver.Firefox())
    ajax_browsers[i].set_page_load_timeout(15)

def fetch(url, browser, i):
    try:
        browser.get(url)
        n = browser.page_source
        soup = BeautifulSoup(n)
        n = soup.get_text()
        try:
            n = n.encode('utf-8')
        except:
            d = chardet.detect(n)
            n = n.decode(d['encoding']).encode('utf-8')
    except TimeoutException:
        n = browser.page_source
        soup = BeautifulSoup(n)
        n = soup.get_text()
        try:
            n = n.encode('utf-8')
        except:
            d = chardet.detect(n)
            n = n.decode(d['encoding']).encode('utf-8')
        n = 'TIMEDOUT'+n
    except WebDriverException as error:
        if 'MALFORMED_URI' in error.msg:
            n = 'MALFORMED_URI'
        else:
            raise error
    except Exception, error:
        raise error
    #return n.decode(d['encoding']).encode('utf-8'),
    return i,n

from multiprocessing import Pool

def getajax(candidate, webpage, state, district_type, district_name):
    non_websites = getlinks(candidate, webpage, state, district_type, district_name)
    import pdb;pdb.set_trace()
    non_websites_text = ['no_result']*num_sites
    def callb(ni):
        print 'adding ' + str(ni[0])
        non_websites_text[ni[0]] = ni[1]
    pool = Pool(processes=num_sites)
    for i in range(len(non_websites)):
        url = non_websites[i]
        print 'processing '+ str(i)
        pool.apply_async(fetch, [url, ajax_browsers[i],i],callback=callb)
    pool.close()
    pool.join()
    return tuple(non_websites_text)

DEFAULT_TABLE = {
        'skip_head_lines':0,
        'format':'csv',
        'field_sep':',',
        'quotechar':'"',
        'copy_every':100,
        'udcs':{
            },
        }

SITES_AJAX = dict(DEFAULT_TABLE)
SITES_AJAX.update({
    'table':'non_sites_ajax',
    'filename':'/home/gaertner/code/candclass/webpages_ex.csv',
    'field_sep':',',
    'columns':{
        'uid':1,
        'name':2,
        'state':4,
        'electoral_district_type':5,
        'electoral_district_name':6,
        tuple('sitetext{i}'.format(i=i) for i in range(1,num_sites+1)):{'function':getajax,'columns':(2,3,4,5,6)},
        },
    })

SITES_PLAIN = dict(DEFAULT_TABLE)
SITES_PLAIN.update({
    'table':'non_sites_plain',
    'filename':'/home/gaertner/code/candclass/webpages_ex.csv',
    'field_sep':',',
    'columns':{
        'uid':1,
        'name':2,
        'state':4,
        'electoral_district_type':5,
        'electoral_district_name':6,
        tuple('sitetext{i}'.format(i=i) for i in range(1,num_sites+1)):{'function':getplain,'columns':(2,3,4,5,6)},
        },
    })
ERSATZPG_CONFIG.update({
    #    'use_utf':True,
    'tables':{
        'non_sites_ajax':SITES_AJAX,
        'non_sites_plain':SITES_PLAIN,
        },
    'parallel_load':({'tables':('non_sites_ajax','non_sites_plain'),'keys':{}},),
    'key_sources':{},
    })

