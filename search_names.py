import psycopg2, csv, re
from cStringIO import StringIO


connstr = "dbname=%s user=%s password=%s" % ('bip4', 'postgres', '|-|3lp3rb34r')
connection = psycopg2.connect(connstr)
cursor = connection.cursor()
name_pat = re.compile(r'(?P<first>\w+)\s+(?:\w\.?\s+)?(:?"(?P<nick>\w+)"\s+)?(?P<last>\w+)(?:\s+jr\.|ii|iii|iv|sr\.)?')
initial_first_pat = re.compile(r'(?:\w\.?)\s+(?P<first>\w+)\s+(:?"(?P<nick>\w+)"\s+)?(?P<last>\w+)(?:\s+jr\.|ii|iii|iv|sr\.)?')
def candidate_convert(candidate):
    candidate = candidate.lower().strip()
    m = initial_first_pat.match(candidate)
    if not m:
        m = name_pat.match(candidate)
    if m:
        if m.groupdict()['nick']:
            candidate = '{first} {last}'.format(first=m.groupdict()['nick'].strip(),last=m.groupdict()['last'])
        else:
            candidate = '{first} {last}'.format(first=m.groupdict()['first'],last=m.groupdict()['last'])
    else:
        print 'no match'
        if len(candidate.strip().split(' ')) > 1:
            candidate = candidate.strip().split(' ')[0] + ' ' + candidate.strip().split(' ')[-1]
        else:
            candidate = candidate.strip()
    return candidate

with open('names_to_run.csv') as f, open('names_matched.csv','w') as g:
    buf = StringIO()
    writer = csv.writer(buf)
    reader = csv.reader(f)
    for l in reader:
        writer.writerow(map(candidate_convert, l))
    buf.seek(0)
    print buf.read()
    buf.seek(0)
    cursor.execute('DROP TABLE IF EXISTS candidate_project')
    cursor.execute('DROP TABLE IF EXISTS candidate_project_ts')
    cursor.execute('DROP TABLE IF EXISTS candidate_ts')
    cursor.execute('CREATE TABLE candidate_project(name text);')
    sql = 'COPY candidate_project(name) from STDOUT WITH CSV;'
    cursor.copy_expert(sql, buf)
    cursor.execute('CREATE TABLE candidate_project_lower as select lower(name) as name from candidate_project;')
    cursor.execute('CREATE INDEX on candidate_project_lower(name);')
    cursor.execute('CREATE TABLE candidate_lower as select lower(name) as name, source from candidate;')
    cursor.execute('CREATE INDEX on candidate_lower(name);')
    #cursor.execute('SELECT candidate_lower.name, candidate_lower.source, candidate_project_lower.name from candidate_lower join candidate_project_lower on candidate_lower.name ~* candidate_project_lower.name;')
    cursor.execute('SELECT name, source from candidate_lower where name in (select name from candidate_project_lower);')
    #cursor.execute('CREATE TABLE candidate_project_ts as select name, plainto_tsquery(name) as quer from candidate_project;')
    #cursor.execute('CREATE TABLE candidate_ts as select name, source, to_tsvector(name) as vec from candidate;')
    #cursor.execute('SELECT candidate_ts.name, candidate_project_ts.name, candidate_ts.source from candidate_ts join candidate_project_ts on candidate_ts.vec @@ candidate_project_ts.quer;')
    #cursor.execute('SELECT candidate.name, candidate.source from candidate join candidate_project on to_tsvector(candidate.name) @@ plainto_tsquery(candidate_project.name)')
    r = cursor.fetchall()
    rwriter = csv.writer(g)
    for l in r:
        rwriter.writerow(l)
    cursor.execute('DROP TABLE IF EXISTS candidate_project')
    cursor.execute('DROP TABLE IF EXISTS candidate_project_ts')
    cursor.execute('DROP TABLE IF EXISTS candidate_ts')
    connection.commit()
    connection.close()
