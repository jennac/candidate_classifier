from univ_settings import ERSATZPG_CONFIG
from collections import OrderedDict
import sys, os
import urllib2 as ul
import urllib3
from urllib2 import URLError
from selenium import webdriver
from selenium.common.exceptions import WebDriverException, TimeoutException
from bs4 import BeautifulSoup
from pagecache import Cache
import pagecache
import re, chardet
from ersatzpg import ersatz
from multiprocessing import Lock
from multiprocessing import Queue
from locked_object_list import Queue as MyQueue
import random

http = urllib3.PoolManager()
random.seed(1121)
site_log = open('sites_fetched', 'w')
pass_errors = False
num_processes = 40
connection_pool_size = 2
cache_list = []
queue = Queue(num_processes)
for i in range(num_processes):
    c = Cache()
    cache_list.append(c)
    queue.put(c)
#queue = Queue(cache_list)
cache_lock = Lock()
conn_queue = None
conn_list = None
conn_queue_idxs = Queue(connection_pool_size)
for i in range(connection_pool_size):
    conn_queue_idxs.put(i)

def fetchplain(url):
    site_log.write('getting {url}\n'.format(url=url))
    try:
        global conn_list
        cache = queue.get()
        r,htype = cache.get(url, conn_list, conn_queue_idxs,10)
        #site = ul.urlopen(url, timeout=10)
        #if site.headers.gettype() == 'text/html':
        #    soup_text = BeautifulSoup(site.read()).get_text()
        if htype == 'text/html':
            soup_text = BeautifulSoup(r).get_text()
        else:
            soup_text = ''
    except ul.HTTPError:
        soup_text='httperror'
    except Exception as error:
        print 'ERROR: {error}'.format(error=error)
        soup_text = 'othererror'
    finally:
        queue.put(cache)
    return soup_text, url

from multiprocessing import Pool, Lock
pattern = re.compile(r'https?://.+')
def getplain(url):
    url = url.decode('utf-8').strip()
    print url
    site_log.write(url+'\n')
    #url.encode(chardet.detect(url)['encoding'])
    if not pattern.match(url):
        #url_for_split = 'http://'+url
        pass
    else:
        #url for_split = url
        url = url.replace('http://','')
    try:
        #site = ul.urlopen(url, timeout=30)
        global conn_list
        conn_list = []
        for i in range(connection_pool_size):
            conn = urllib3.connection_from_url(url, timeout=30)
            conn_list.append(conn)
        conn_idx = conn_queue_idxs.get()
        conn = conn_list[conn_idx]
        print url
        site = conn.urlopen('GET',url, timeout=30)
        conn_queue_idxs.put(conn_idx)
        #url = site.geturl()
        url = site.get_redirect_location() if site.get_redirect_location() else url
        if not pattern.match(url):
            url = 'http://'+url
        split_url = ul.urlparse.urlsplit(url)
        domain = split_url.netloc.lower()
        domain_no_www = re.match(r'(?:www.)?(?P<domain>.+)', domain).groupdict()['domain']
        path = split_url.path.split('/')
        #path = [p.lower() for p in path]
        if '.' in path[-1]:
            path = path[:-1]
        relative_root = 'http://' + '/'.join([domain]+(path if path[0] != '' else []))
        #n = site.read()
        n = site.data
        soup = BeautifulSoup(n)
        if soup.title != None and soup.title.get_text() == 'Zephyr - August 2010 Template Demo':
            return 'PAGE REDIRECTS TO BAD SITE',
        bad_rel = ['javascript:','mailto:']
        relative_links = set([l.get('href') for l in soup.find_all('a') if not l.get('href') == None and not l.get('href') == '/' and not l.get('href').lower().startswith('http') and not any(map(lambda text:text in l.get('href').lower(), bad_rel))])
        absolute_links = set([l.get('href') for l in soup.find_all('a') if l.get('href') != None and domain_no_www in l.get('href').lower() and l.get('href').lower().startswith('http')])
        relative_links = set([relative_root + '/' + l.lstrip('/') for l in relative_links if (relative_root + '/' + l.lstrip('/')).lower() not in [a.lower() for a in absolute_links]])

        relative_frames = set([l.get('src') for l in soup.find_all('frame') if not l.get('src') == None and not l.get('src') == '/' and not l.get('src').lower().startswith('http') and not any(map(lambda text:text in l.get('src').lower(), bad_rel))])
        absolute_frames = set([l.get('src') for l in soup.find_all('frame') if l.get('src') != None and l.get('src').lower().startswith('http')])
        relative_frames = set([relative_root + '/' + l.lstrip('/') for l in relative_frames if (relative_root + '/' + l.lstrip('/')).lower() not in [a.lower() for a in absolute_frames]])
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
        pool = Pool(processes = num_processes)
        used_links = list(relative_links.union(absolute_links))
        random.shuffle(used_links)
        used_links = used_links[:50]

        for l in used_links:
            lname, lext = os.path.splitext(l)
            if lext not in ['html','htm','php','']:
                continue
            pool.apply_async(fetchplain, [l], callback=callb)
        pool.close()
        pool.join()
        n = ''.join(n)
        for l in relative_frames.union(absolute_frames):
            lname, lext = os.path.splitext(l)
            if lext not in ['html','htm','php','']:
                continue
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
    return n,url

def get_page_for_class(url):
    url = url.decode('utf-8').strip()
    print url
    site_log.write(url+'\n')
    #url.encode(chardet.detect(url)['encoding'])
    try:
        if not pattern.match(url):
            url = 'http://'+url
        try:
            conn = urllib3.connection_from_url(url, timeout=30)
            site = conn.urlopen('GET',url, timeout=30)
            n = site.data
            url = site.get_redirect_location() if site.get_redirect_location() else url
        except:
            site = ul.urlopen(url, timeout=30)
            n = site.read()
            url = site.geturl()
        split_url = ul.urlparse.urlsplit(url)
        domain = split_url.netloc.lower()
        domain_no_www = re.match(r'(?:www.)?(?P<domain>.+)', domain).groupdict()['domain']
        path = split_url.path.split('/')
        #path = [p.lower() for p in path]
        if '.' in path[-1]:
            path = path[:-1]
        relative_root = 'http://' + '/'.join([domain]+(path if path[0] != '' else []))
        #n = site.read()
        soup = BeautifulSoup(n)
        if soup.title != None and soup.title.get_text() == 'Zephyr - August 2010 Template Demo':
            return 'PAGE REDIRECTS TO BAD SITE',
        bad_rel = ['javascript:','mailto:']
        relative_links = set([l.get('href') for l in soup.find_all('a') if not l.get('href') == None and not l.get('href') == '/' and not l.get('href').lower().startswith('http') and not any(map(lambda text:text in l.get('href').lower(), bad_rel))])
        absolute_links = set([l.get('href') for l in soup.find_all('a') if l.get('href') != None and domain_no_www in l.get('href').lower() and l.get('href').lower().startswith('http')])
        relative_links = set([relative_root + '/' + l.lstrip('/') for l in relative_links if (relative_root + '/' + l.lstrip('/')).lower() not in [a.lower() for a in absolute_links]])

        relative_frames = set([l.get('src') for l in soup.find_all('frame') if not l.get('src') == None and not l.get('src') == '/' and not l.get('src').lower().startswith('http') and not any(map(lambda text:text in l.get('src').lower(), bad_rel))])
        absolute_frames = set([l.get('src') for l in soup.find_all('frame') if l.get('src') != None and l.get('src').lower().startswith('http')])
        relative_frames = set([relative_root + '/' + l.lstrip('/') for l in relative_frames if (relative_root + '/' + l.lstrip('/')).lower() not in [a.lower() for a in absolute_frames]])
        n = [soup.get_text()]
        if 'This Web page is parked for FREE, courtesy of' in n:
            return 'PAGE REDIRECTS TO GODADDY',
        used_links = list(relative_links.union(absolute_links))
        linkwords = []
        for link in used_links:
            slink = ul.urlparse.urlsplit(link)
            try:
                linkwords += map(lambda s: re.match(r'(?P<name>[^\.]+)(?:\.\w+)?',s).groupdict()['name'] if re.match(r'(?P<name>[^\.]+)(?:\.\w+)?',s) else s,re.split(r'[/-_]',slink.path))
            except:
                import pdb; pdb.set_trace()
        n = ''.join(n)
        n += ' '.join(linkwords)
        for l in relative_frames.union(absolute_frames):
            lname, lext = os.path.splitext(l)
            if lext not in ['html','htm','php','']:
                continue
            print 'getting frame at {l}'.format(l=l)
            n += get_page_for_class(l)[0]
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
    return n,url
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
        if pass_errors:
            n = 'ERROR'
        else:
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
    'table':'new_sites_plain',
    'filename':'/home/gaertner/code/candclass/webpages.csv',
    'field_sep':',',
    'columns':{
        'uid':1,
        ('sitetext','website'):{'function':getplain,'columns':(2,)},
        },
    })
ERSATZPG_CONFIG.update({
    'use_utf':False,
    'tables':{
        #'sites_ajax':SITES_AJAX,
        'new_sites_plain':SITES_PLAIN,
        },
    #'parallel_load':({'tables':('sites_ajax','sites_plain'),'keys':{}},),
    'parallel_load':(),
    'key_sources':{},
    })

