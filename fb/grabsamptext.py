import csv
import random
from itertools import izip
with open('full_classifier_outs.csv') as f, open('srsplit/fullfbsrc2.csv') as g, open('sampletext.csv','w') as h, open('fullfbcands.csv') as k:
    csv_ratings = csv.DictReader(f)
    csv_canddata = csv.DictReader(k)
    csv_texts = csv.DictReader(g)
    csv_samples = csv.DictWriter(h, ['uid','link','score','name','office','electoral_district_name','sitetext'])
    ratings_dict = dict(((l['uid'], l['link']),int(l['sum'])) for l in csv_ratings)
    data_dict = dict((l['identifier'], {'name':l['name'],'office':l['office'],'edn':l['electoral_district_name']}) for l in csv_canddata)
    csv_samples.writeheader()
    for l in csv_texts:
        score = ratings_dict[(l['uid'], l['link'])]
        if data_dict.has_key(l['uid']):
            data = data_dict[l['uid']]
        else:
            continue
        if score >= 9 and random.random() > .9:
            #if score >= 9:
            csv_samples.writerow({'uid':l['uid'],'name':data['name'],'office':data['office'],'electoral_district_name':data['edn'],'link':l['link'],'sitetext':l['sitetext'],'score':score})

