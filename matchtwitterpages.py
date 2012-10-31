import csv, os, sys, re
from passdict import passdict
from collections import defaultdict
import conversions
threshold = .8
cands = [d for d in os.listdir('/home/gaertner/Dropbox/BIP Production') if re.match(r'\w\w Candidates(?:-P)?.csv',d)]
with open('twitter/full_classifier_outs.csv') as f:
    csvr = csv.reader(f)
    fb_dict = defaultdict(lambda:{'twitterprob':list(),'twitterlink':list()})
    csvr.next()
    for l in csvr:
        if float(l[0]) < threshold:
            continue
        fb_dict[l[15]]['twitterprob'].append(l[0])
        fb_dict[l[15]]['twitterlink'].append(conversions.web_to_twitter_handle(l[16]))
os.chdir('/home/gaertner/Dropbox/BIP Production')
for c in cands:
    with open(c, 'rU') as g, open(os.path.join('/home/gaertner/code/candclass/twitter/new_candidates',c),'w') as h:
        csvr = csv.DictReader(g)
        csvw = csv.DictWriter(h, csvr.fieldnames+['fbprob','fblink','webprob','weblink','twitterprob','twitterlink'])
        csvw.writeheader()
        for l in csvr:
            try:
                l.update(fb_dict[l['UID']])
                if l.has_key(None):
                    l.pop(None)
                csvw.writerow(l)
            except Exception as error:
                import pdb;pdb.set_trace()
