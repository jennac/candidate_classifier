import urllib2 as ul
import csv
from collections import Counter

with open('nonwebpages.csv') as f:
#, open('page_counts.csv','w') as g:
    csvr = csv.DictReader(f)
    #csvw = csv.writer(g)
    nwl = []
    for l in csvr:
        uid = l['uid']
        nwl += [ul.urlparse.urlparse(a).netloc for b in eval(l['non_webpage_list']) for a in b]
    print Counter(nwl).most_common(50)
