from univ_settings import ERSATZPG_CONFIG
from collections import OrderedDict
import sys
import urllib2 as ul
from urllib2 import URLError
from selenium import webdriver
from selenium.common.exceptions import WebDriverException, TimeoutException
from bs4 import BeautifulSoup
import re, chardet

pattern = re.compile(r'http://.+')
def getplain(url):
    print url
    if not pattern.match(url):
        url = 'http://'+url
    try:
        site = ul.urlopen(url, timeout=30)
        split_url = ul.urlparse.urlsplit(url)
        domain = split_url.netloc
        path = split_url.path.split('/')
        if '.' in path[-1]:
            path = path[:-1]
        relative_root = 'http://' + '/'.join([domain]+(path if path[0] != '' else []))
        n = site.read()
        soup = BeautifulSoup(n)
        if soup.title.get_text() == 'Zephyr - August 2010 Template Demo':
            return 'PAGE REDIRECTS TO BAD SITE'
        relative_links = [l.get('href') for l in soup.find_all('a') if not l.get('href') == None and not l.get('href').startswith('http')]
        absolute_links = [l.get('href') for l in soup.find_all('a') if l.get('href') != None and  domain in l.get('href') and l.get('href').startswith('http')]
        relative_links = [relative_root + '/' + l.lstrip('/') for l in relative_links if relative_root + '/' + l.lstrip('/') not in absolute_links]
        n = soup.get_text()
        if 'This Web page is parked for FREE, courtesy of' in n:
            return 'PAGE REDIRECTS TO GODADDY'
        for l in relative_links + absolute_links:
            site = ul.urlopen(l, timeout=10)
            soup = BeautifulSoup(site.read())
            n += soup.get_text()
            if sys.getsizeof(n) > 2**22:
                break
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
    return n,

#browser = webdriver.Firefox()
#browser.set_page_load_timeout(30)
def getajax(url):
    if not pattern.match(url):
        url = 'http://' + url
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
    return n,

DEFAULT_TABLE = {
        'skip_head_lines':0,
        'format':'csv',
        'field_sep':',',
        'quotechar':'"',
        'copy_every':100,
        'udcs':{
            },
        }
"""
SITES_AJAX = dict(DEFAULT_TABLE)
SITES_AJAX.update({
    'table':'sites_ajax',
    'filename':'/home/gaertner/code/candclass/webpages.csv',
    'field_sep':',',
    'columns':{
        'uid':1,
        'website':2,
        'sitetext':{'function':getajax,'columns':(2,)},
        },
    })
"""
SITES_PLAIN = dict(DEFAULT_TABLE)
SITES_PLAIN.update({
    'table':'sites_plain',
    'filename':'/home/gaertner/code/candclass/webpages.csv',
    'field_sep':',',
    'columns':{
        'uid':1,
        'website':2,
        'sitetext':{'function':getplain,'columns':(2,)},
        },
    })
ERSATZPG_CONFIG.update({
    'use_utf':True,
    'tables':{
        #'sites_ajax':SITES_AJAX,
        'sites_plain':SITES_PLAIN,
        },
    #'parallel_load':({'tables':('sites_ajax','sites_plain'),'keys':{}},),
    'parallel_load':(),
    'key_sources':{},
    })

