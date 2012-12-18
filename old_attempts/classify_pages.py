import psycopg2.psycopg1 as psycopg
from univ_settings import DATABASE_CONFIG as config
from collections import defaultdict
import csv
from cStringIO import StringIO
from ersatzpg.utffile import utffile

connstr = []
if config.has_key('host'):
    connstr.append("host=%s" % config['host'])
if config.has_key('port'):
    connstr.append("port=%s" % config['port'])
if config.has_key('sslmode'):
    connstr.append("sslmode=%s" % config['sslmode'])
connstr.append("dbname=%s user=%s password=%s" % (config['db'], config['user'], config['pw']))
connection = psycopg.connect(' '.join(connstr))
cursor = connection.cursor()
results = defaultdict(lambda:[])
special_terms = []
with utffile('searchterms.csv') as f:
    for s in f:
        print s
        if s.startswith('<'):
            special_terms.append(s)
        elif ' ' in s:
            #cursor.execute("select identifier, sitetext ilike '%{s}%' from sites_ajax_joined;".format(s=s))
            cursor.execute("select uid, class, website, sitevec @@ plainto_tsquery('{s}') from pages_for_class_train_vec;".format(s=s))
            for k,c,w,v in cursor.fetchall():
                results[(k,c,w)].append(int(v))
        else:
            cursor.execute("select uid, class, website, sitevec @@ to_tsquery('{s}') from pages_for_class_train_vec;".format(s=s))
            for k,c,w,v in cursor.fetchall():
                results[(k,c,w)].append(int(v))
cursor.execute('DROP TABLE IF EXISTS pages_classed;')
cursor.execute('CREATE TABLE pages_classed(uid varchar(10), class varchar(10), website text, classifier text);')
sql = "COPY pages_classed(uid, class, website, classifier) from STDOUT WITH CSV"
buf = StringIO()
writer = csv.writer(buf)
sum_true_vecs = []
sum_false_vecs = []
for k,v in results.iteritems():
    k,c,w = k
    if c == 'True':
        sum_true_vecs.append(sum(v))
    else:
        sum_false_vecs.append(sum(v))
    if all(map(lambda x: x==0,v)):
        writer.writerow([k,c,w,'none'])
    else:
        writer.writerow([k,c,w,repr(v)])
buf.seek(0)
cursor.copy_expert(sql, buf)
connection.commit()
connection.close()
print sum(sum_true_vecs)/len(sum_true_vecs)
print sum(sum_false_vecs)/len(sum_false_vecs)
