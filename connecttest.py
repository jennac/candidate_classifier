import psycopg2 as ps
from univ_settings import ERSATZPG_CONFIG as ec
from ersatzpg import ersatz
import pagecache as pc

c1 = pc.db_connect(ec)
c2 = pc.db_connect(ec)
print c1.fileno()
print c2.fileno()
c3 = ersatz.db_connect(ec)
print c3.fileno()
print c1.fileno()
print c2.fileno()
