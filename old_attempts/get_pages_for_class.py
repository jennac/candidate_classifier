from ersatzpg import ersatz
from univ_settings import ERSATZPG_CONFIG
from collections import OrderedDict
import sys, os,csv, time
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
import get_links
import os, re, subprocess
from cStringIO import StringIO
import psycopg2
pass_errors = False
random.seed(1121)
site_log = open('sites_fetched', 'w')
pattern = re.compile(r'https?://.+')
def get_page_for_class(url,lprime):
    url = url.decode('utf-8').strip()
    #print url
    site_log.write(url+'\n')
    #url = url.encode(chardet.detect(url)['encoding'])
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
            n += get_page_for_class(l,lprime)[0]
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
    return n,url,lprime

from multiprocessing import Pool
os.chdir('sr')
webpages = [d for d in os.listdir('.') if re.match(r'search_results\d\d+',d)]
webpages.sort()
connection = ersatz.db_connect(ERSATZPG_CONFIG)
cursor = connection.cursor()
for w in webpages:
    print w
    with open(w) as f:
        t = time.time()
        pool = Pool(processes=10)
        buf = StringIO()
        csvw = csv.writer(buf)
        csvr = csv.reader(f)
        def callb(nurl):
            n,url,lprime=nurl
            csvw.writerow([lprime[0],lprime[3],url,n])
        csvr.next()
        for l in csvr:
            pool.apply_async(get_page_for_class,[l[1],l], callback=callb)
        pool.close()
        pool.join()
        buf.seek(0)
        sql = 'COPY pages_for_class(uid,class,website,sitetext) from STDOUT with CSV'
        cursor.copy_expert(sql, buf)
        connection.commit()
        print "time elapsed: {t}".format(t=time.time()-t)

    pipe = subprocess.Popen(['mv',w,'done_sr'+w[-2:]],stdin=subprocess.PIPE)
    pipe.wait()

