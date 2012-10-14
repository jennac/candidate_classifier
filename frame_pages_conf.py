from univ_settings import ERSATZPG_CONFIG
from collections import OrderedDict
import sys
import urllib2 as ul
from urllib2 import URLError
from selenium import webdriver
from selenium.common.exceptions import WebDriverException, TimeoutException
from bs4 import BeautifulSoup
import re, chardet
pass_errors=False
def fetchplain(url):
    print 'getting {url}'.format(url=url)
    try:
        site = ul.urlopen(url, timeout=10)
        if site.headers.gettype() == 'text/html':
            soup_text = BeautifulSoup(site.read()).get_text()
        else:
            soup_text = ''
    except ul.HTTPError:
        soup_text='httperror'
    return soup_text, url

from multiprocessing import Pool, Lock
pattern = re.compile(r'http://.+')
def getplain(url):
    url = url.decode('utf-8').strip()
    print url
    if not pattern.match(url):
        url = 'http://'+url
    try:
        site = ul.urlopen(url, timeout=30)
        split_url = ul.urlparse.urlsplit(url)
        domain = split_url.netloc.lower()
        domain_no_www = re.match(r'(?:www.)?(?P<domain>.+)', domain).groupdict()['domain']
        path = split_url.path.split('/')
        #path = [p.lower() for p in path]
        if '.' in path[-1]:
            path = path[:-1]
        relative_root = 'http://' + '/'.join([domain]+(path if path[0] != '' else []))
        n = site.read()
        soup = BeautifulSoup(n)
        if soup.title != None and soup.title.get_text() == 'Zephyr - August 2010 Template Demo':
            return 'PAGE REDIRECTS TO BAD SITE',
        bad_rel = ['javascript:','mailto:']
        relative_links = set([l.get('href') for l in soup.find_all('a') if not l.get('href') == None and not l.get('href') == '/' and not l.get('href').lower().startswith('http') and not any(map(lambda text:text in l.get('href').lower(), bad_rel))])
        absolute_links = set([l.get('href') for l in soup.find_all('a') if l.get('href') != None and domain_no_www in l.get('href').lower() and l.get('href').lower().startswith('http')])
        relative_links = set([relative_root + '/' + l.lstrip('/') for l in relative_links if (relative_root + '/' + l.lstrip('/')).lower() not in [a.lower() for a in absolute_links]])
        n = [soup.get_text()]
        if 'This Web page is parked for FREE, courtesy of' in n:
            return 'PAGE REDIRECTS TO GODADDY',
        lock = Lock()
        def callb(m):
            url = m[1]
            m = m[0]
            lock.acquire()
            n.append(m)
            if sys.getsizeof(n) > 2**22:
                pool.terminate()
            lock.release()
        pool = Pool(processes = 4)
        for l in relative_links.union(absolute_links):
            #site = ul.urlopen(l, timeout=10)
            #soup = BeautifulSoup(site.read())
            #n += soup.get_text()
            #if sys.getsizeof(n) > 2**22:
            #    break
            pool.apply_async(fetchplain, [l], callback=callb)
        pool.close()
        pool.join()
        n = ''.join(n)
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
            import traceback; print traceback.format_exc()
            raise error
    #return n.decode(d['encoding']).encode('utf-8'),
    return n,

def getframedata(url):
    url = url.decode('utf-8').strip()
    print url
    if not pattern.match(url):
        url = 'http://'+url
    try:
        site = ul.urlopen(url, timeout=30)
        split_url = ul.urlparse.urlsplit(url)
        domain = split_url.netloc.lower()
        domain_no_www = re.match(r'(?:www.)?(?P<domain>.+)', domain).groupdict()['domain']
        path = split_url.path.split('/')
        #path = [p.lower() for p in path]
        if '.' in path[-1]:
            path = path[:-1]
        relative_root = 'http://' + '/'.join([domain]+(path if path[0] != '' else []))
        n = site.read()
        soup = BeautifulSoup(n)
        if soup.title != None and soup.title.get_text() == 'Zephyr - August 2010 Template Demo':
            return 'PAGE REDIRECTS TO BAD SITE',
        bad_rel = ['javascript:','mailto:']
        relative_frames = set([l.get('src') for l in soup.find_all('frame') if not l.get('src') == None and not l.get('src') == '/' and not l.get('src').lower().startswith('http') and not any(map(lambda text:text in l.get('src').lower(), bad_rel))])
        absolute_frames = set([l.get('src') for l in soup.find_all('frame') if l.get('src') != None and l.get('src').lower().startswith('http')])
        relative_frames = set([relative_root + '/' + l.lstrip('/') for l in relative_frames if (relative_root + '/' + l.lstrip('/')).lower() not in [a.lower() for a in absolute_frames]])
        n = ''
        for l in relative_frames.union(absolute_frames):
            print 'getting frame at {l}'.format(l=l)
            n += getplain(l)[0]
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
            if pass_errors:
                n = 'ERROR'
            else:
                import traceback; print traceback.format_exc()
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
        'copy_every':25,
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
    'table':'frame_sites_plain',
    'filename':'/home/gaertner/code/candclass/webpages.csv',
    'field_sep':',',
    'columns':{
        'uid':1,
        'website':2,
        'sitetext':{'function':getframedata,'columns':(2,)},
        },
    })
ERSATZPG_CONFIG.update({
    'use_utf':False,
    'tables':{
        #'sites_ajax':SITES_AJAX,
        'frame_sites_plain':SITES_PLAIN,
        },
    #'parallel_load':({'tables':('sites_ajax','sites_plain'),'keys':{}},),
    'parallel_load':(),
    'key_sources':{},
    })

