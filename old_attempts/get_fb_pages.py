from ersatzpg import ersatz
from univ_settings import ERSATZPG_CONFIG
from collections import OrderedDict
import sys, os,csv, time
import urllib2 as ul
import urllib3
from urllib3.connectionpool import HostChangedError
from urllib2 import URLError
from selenium import webdriver
from selenium.common.exceptions import WebDriverException, TimeoutException
from bs4 import BeautifulSoup, Comment
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
import requests
from fblogin import logindata
pass_errors = False
random.seed(1121)
site_log = open('sites_fetched', 'w')
pattern = re.compile(r'https?://.+')
ua_header = {'user-agent': 'Python-urllib/2.7'}
num_tries = 5
loginurl = 'https://login.facebook.com/login.php'
r = requests.post(loginurl, data=logindata)
r = requests.post(loginurl, data=logindata, cookies=r.cookies)
logincookie=r.cookies
def renewlogin():
    global logincookie
    r = requests.post(loginurl, data=logindata)
    r = requests.post(loginurl, data=logindata, cookies=r.cookies)
    logincookie=r.cookies



def get_fb_home_text(site_source, get_links=False):
    b = BeautifulSoup(site_source)
    comments = map(lambda c: BeautifulSoup(c.string),b.findAll(text=lambda text:isinstance(text, Comment)))
    #text = b.getText(' ') + ' '.join(c.getText(' ') for c in comments)
    text = ' '.join(c.getText(' ') for c in comments)
    useful_urls = []
    if get_links:
        comment_links = [(a.getText(),a.get('href')) for c in comments for a in c.find_all('a')]
        useful_links = ['About','Notes']
        useful_urls = filter(lambda a: any(map(lambda l:l in a,useful_links)),comment_links)
    return text, useful_urls

import sys
def get_page_for_class(url,lprime):
    url = url.decode('utf-8').strip()
    #print url
    sys.stdout.write('.')
    sys.stdout.flush()
    site_log.write(url+'\n')
    #url = url.encode(chardet.detect(url)['encoding'])
    try:
        if not pattern.match(url):
            url = 'http://'+url
        try:
            for i in range(num_tries):
                #conn = urllib3.connection_from_url(url, timeout=30)
                #site = conn.urlopen('GET',url, headers=ua_header, timeout=30)
                #if site.status == 200:
                    #source = site.data
                    #break
                site = requests.get(url, cookies=logincookie)
                if site.status_code == 200:
                    source = site.text
                    n, links = get_fb_home_text(source, True)
                    if n == '':
                        print 'renewing login'
                        renewlogin()
                        site = requests.get(url, cookies=logincookie)
                        if site.status_code == 200:
                            source = site.text
                            n, links = get_fb_home_text(source, True)
                    break
            else:
                n = 'COULD NOT LOAD'
                print 'COULD NOT LOAD ' + url
                return n,url,lprime,[]
            #url = site.get_redirect_location() if site.get_redirect_location() else url
        except Exception as e:
            print e
            print 'using urllib2'
            for i in range(num_tries):
                site = ul.urlopen(url, timeout=30)
                if site.getcode() == 200:
                    source = site.read()
                    break
            else:
                n= 'COULD NOT LOAD'
                return n,url,lprime,[]
            url = site.geturl()

        n, links = get_fb_home_text(source, True)
        return_source = [source]
        if n == '':
            print 'WARNING NO COMMENTS MIGHT BE CAPTCHABLOCKED ' + url
        links = filter(lambda link: re.match(r'^https?://.+',link[1]) != None,links)
        for link in links:
            try:
                for i in range(num_tries):
                    #try:
                        #site = conn.urlopen('GET',link[1], headers=ua_header, timeout=30)
                    #except HostChangedError:
                        #conn = urllib3.connection_from_url(url, timeout=30)
                        #site = conn.urlopen('GET',link[1], headers=ua_header, timeout=30)
                    #except Exception as error:
                        #raise error
                    #if site.status == 200:
                        #source = site.data
                        #break
                    site = requests.get(link, cookies=logincookie)
                    if site.status_code == 200:
                        source = site.text
                        break
                else:
                    source = 'COULD NOT LOAD LINK'
            except:
                for i in range(num_tries):
                    site = ul.urlopen(link[1], timeout=30)
                    if site.getcode() == 200:
                        source = site.read()
                        break
                else:
                    source = 'COULD NOT LOAD LINK'
            n += ' ' + get_fb_home_text(source)[0]
            return_source.append(source)

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
    return n,url,lprime, return_source
if __name__=='__main__':
    from multiprocessing import Pool
    os.chdir('fb')
    webpages = [d for d in os.listdir('.') if re.match(r'fbsearch_results\d\d+',d)]
    webpages.sort()
    connection = ersatz.db_connect(ERSATZPG_CONFIG)
    cursor = connection.cursor()
    for w in webpages:
        print w
        with open(w) as f:
            t = time.time()
            pool = Pool(processes=20)
            buf = StringIO()
            csvw = csv.writer(buf)
            csvr = csv.reader(f)
            def callb(nurl):
                n,url,lprime,return_source=nurl
                csvw.writerow([lprime[0],lprime[2],url,n,repr(return_source)])
            csvr.next()
            for l in csvr:
                pool.apply_async(get_page_for_class,[l[1],l], callback=callb)
            pool.close()
            pool.join()
            buf.seek(0)
            sql = 'COPY fb_pages_for_class(uid,class,website,sitetext,fullsource) from STDOUT with CSV'
            cursor.copy_expert(sql, buf)
            connection.commit()
            print "time elapsed: {t}".format(t=time.time()-t)

        pipe = subprocess.Popen(['mv',w,'done_sr'+w[-2:]],stdin=subprocess.PIPE)
        pipe.wait()
    with open('fberrors.csv') as fe:
        csvw = csw.writer(fe)
        csvw.writerow(['uid','website','class','sitetext'])
        sql = 'select uid, website, class,sitetext from fb_pages_for_class where sitetext is null;'
        cursor.execute(sql)
        for r in cursor.fetchall():
            csvw.writerow(r)
        sql = "select uid, website, class,sitetext from fb_pages_for_class where sitetext='MALFORMED_URI';"
        cursor.execute(sql)
        for r in cursor.fetchall():
            csvw.writerow(r)
        sql = "select uid, website, class,sitetext from fb_pages_for_class where sitetext='TIMEDOUT';"
        cursor.execute(sql)
        for r in cursor.fetchall():
            csvw.writerow(r)
        sql = "select uid, website, class,sitetext from fb_pages_for_class where sitetext='COULD NOT LOAD';"
        cursor.execute(sql)
        for r in cursor.fetchall():
            csvw.writerow(r)
    sql = "delete from fb_pages_for_class where sitetext is null or sitetext='MALFORMED_URI' or sitetext='TIMEDOUT' or sitetext='COULD NOT LOAD';"
    cursor.execute(sql)
    connection.commit()
    connection.close()


