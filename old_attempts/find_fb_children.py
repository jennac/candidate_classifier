import psycopg2
import psycopg2.extras
import re, csv
from ersatzpg import ersatz
from univ_settings import ERSATZPG_CONFIG
from cStringIO import StringIO

def id_children(pages):
    #if pages[0]['uid'] == 'PA00329':
        #import pdb;pdb.set_trace()
    p = filter(lambda p:p['class']=='True',pages)
    if len(p) > 0:
        true_page = p[0]['website']
        webpage_stripped = re.match(r'(?:https?://)?(?:www\.)?(?P<content>.+)',true_page).groupdict()['content'].rstrip('/')
        patt = re.compile(r'^https?://(?:www.)?{webpage}.+'.format(webpage=webpage_stripped.lower()))
        for p in pages:
            if patt.match(p['website']):
                p['class'] = 'Child'
    return pages

connection = ersatz.db_connect(ERSATZPG_CONFIG)
cursor = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)

sql = 'select uid, website, class from fb_pages_for_class;'

cursor.execute(sql)
results = cursor.fetchall()
buf = StringIO()
csvw = csv.writer(buf)
for uid in set(map(lambda r: r['uid'],results)):
    pages = id_children(filter(lambda r: r['uid']==uid,results))
    for p in pages:
        csvw.writerow([p['uid'],p['class'],p['website']])

sql = 'DROP TABLE IF EXISTS fb_page_reclass;'
cursor.execute(sql)
sql = 'CREATE TABLE fb_page_reclass(uid varchar(12), class varchar(10), website text);'
cursor.execute(sql)
sql = 'COPY fb_page_reclass(uid, class, website) from STDOUT WITH CSV'
buf.seek(0)
cursor.copy_expert(sql, buf)
sql = 'DROP TABLE IF EXISTS fb_pages_for_class_reclassed'
cursor.execute(sql)
sql = 'CREATE TABLE fb_pages_for_class_reclassed as select e.uid, e.website, e.sitetext, f.class from fb_pages_for_class as e join fb_page_reclass as f on e.uid=f.uid and e.website = f.website;'
cursor.execute(sql)
sql = 'DROP TABLE IF EXISTS fb_pages_for_class_train;'
cursor.execute(sql)
sql = 'DROP TABLE IF EXISTS fb_pages_for_class_test;'
cursor.execute(sql)
sql = 'CREATE TABLE fb_pages_for_class_train as select * from fb_pages_for_class_reclassed limit 4000;'
cursor.execute(sql)
sql = 'CREATE TABLE fb_pages_for_class_test as select * from fb_pages_for_class_reclassed offset 4000;'
cursor.execute(sql)
connection.commit()
connection.close()
