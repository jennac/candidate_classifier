import psycopg2 as psycopg
from psycopg2.extensions import QuotedString
import urllib2 as ul
import select, chardet
from univ_settings import ERSATZPG_CONFIG as ec


def db_connect(config):
    connstr = []
    if config.has_key('host'):
        connstr.append("host=%s" % config['host'])
    if config.has_key('port'):
        connstr.append("port=%s" % config['port'])
    if config.has_key('sslmode'):
        connstr.append("sslmode=%s" % config['sslmode'])
    connstr.append("dbname=%s user=%s password=%s" % (config['db'], config['user'], config['pw']))
    conn =  psycopg.connect(' '.join(connstr), async=1)
    state = psycopg.extensions.POLL_WRITE
    while state != psycopg.extensions.POLL_OK:
        if state == psycopg.extensions.POLL_WRITE:
            select.select([], [conn.fileno()], [])
        elif state == psycopg.extensions.POLL_READ:
            select.select([conn.fileno()], [], [])
        state = conn.poll()
    return conn

def db_connect_sync(config):
    connstr = []
    if config.has_key('host'):
        connstr.append("host=%s" % config['host'])
    if config.has_key('port'):
        connstr.append("port=%s" % config['port'])
    if config.has_key('sslmode'):
        connstr.append("sslmode=%s" % config['sslmode'])
    connstr.append("dbname=%s user=%s password=%s" % (config['db'], config['user'], config['pw']))
    return psycopg.connect(' '.join(connstr))

def wait(conn):
    while 1:
        state = conn.poll()
        if state == psycopg.extensions.POLL_OK:
            break
        elif state == psycopg.extensions.POLL_WRITE:
            select.select([], [conn.fileno()], [])
        elif state == psycopg.extensions.POLL_READ:
            select.select([conn.fileno()], [], [])
        else:
            raise psycopg.OperationalError("poll() returned %s" % state)

class Cache:
    #def __init__(self, config):
        #self.connection = db_connect(config)
        #wait(self.connection)
        #self.cursor = self.connection.cursor()

    def get(self, url, conn_list, conn_queue_idxs, timeout=30):
        #cursor = self.cursor
        quoted_url = QuotedString(url).getquoted()
        #wait(self.connection)
        try:
            #lock.acquire()
            connection = db_connect(ec)
            wait(connection)
            cursor = connection.cursor()
            cursor.execute('select value,type from cache where key={url}'.format(url=quoted_url))
            wait(cursor.connection)
            try:
                r = cursor.fetchall()
            except Exception as error:
                print error
                r = []
            #lock.release()
        except Exception as error:
            raise error
        finally:
            try:
                #lock.release()
                pass
            except:
                pass
        if len(r) == 0:
            try:
                #site = ul.urlopen(url, timeout=timeout)
                print 'make request'
                conn_idx = conn_queue_idxs.get()
                conn = conn_list[conn_idx]
                print 'got conn'
                print url
                site = conn.urlopen('GET',url, timeout=timeout)
                #site = http_pool.request('GET',url, timeout=timeout)
                print 'request made'
                htype = site.headers['content-type']
                #htype = site.headers.gettype()
                #r = site.read()
                r= site.data
                enc = chardet.detect(r)
                r = r.decode(enc['encoding']).encode('utf-8')
                qr = QuotedString(r).getquoted()
                qt = QuotedString(htype).getquoted()
                #wait(self.connection)
                #lock.acquire()
                connection = db_connect(ec)
                wait(connection)
                cursor = connection.cursor()
                cursor.execute('insert into cache(key,type,value) values({url},{type},{value})'.format(url=quoted_url,type=qt, value=qr))
                wait(cursor.connection)
                #self.connection.commit()
                #connection.commit()
                #lock.release()
            except Exception as error:
                import traceback; print traceback.format_exc()
                raise error
            finally:
                #conn_queue.put(conn)
                conn_queue_idxs.put(conn_idx)
                #print 'put conn'
                try:
                    #lock.release()
                    pass
                except:
                    pass
        else:
            htype = r[0][1]
            r = r[0][0]
        return r,htype
