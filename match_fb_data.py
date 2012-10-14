import conversions
import csv,re
with open('fb/House Races.csv') as hr, open('fb/2012 Senate Races.csv') as sr, open('fb/Gubernatorial Races.csv') as gr, open('fb/Presidential Race.csv') as pr, open('fb/fbcands.csv') as fc, open('fb/morefbcands.csv','w') as mfc:
    csvhr = csv.DictReader(hr)
    csvsr = csv.DictReader(sr)
    csvgr = csv.DictReader(gr)
    csvpr = csv.DictReader(pr)
    csvfc = csv.DictReader(fc)
    csvmfc = csv.DictWriter(mfc, csvfc.fieldnames)
    hrdict = {}
    for l in csvhr:
        hrdict.update({(re.match(r'(?P<state>\w{2})-(?P<number>\d+)',l['DISTRICT']).groupdict()['state'],int(re.match(r'(?P<state>\w{2})-(?P<number>\d+)',l['DISTRICT']).groupdict()['number']),conversions.clean_name(l['CANDIDATE'])):l['URL'].replace('?ref=ts','')})
    srdict = dict(((l['STATE'],conversions.clean_name(l['CANDIDATE'])),l['URL'].replace('?ref=ts','')) for l in csvsr)
    grdict = dict(((l['STATE'],conversions.clean_name(l['CANDIDATE'])),l['URL'].replace('?ref=ts','')) for l in csvgr)
    csvmfc.writeheader()
    for l in csvfc:
        try:
            hrkey=(l['state'], int(l['electoral_district_name']),conversions.clean_name(l['name']))
        except:
            hrkey=(l['state'], l['electoral_district_name'],conversions.clean_name(l['name']))
        srkey=(l['state'],conversions.clean_name(l['name']))
        if hrdict.has_key(hrkey):
            hrdict.pop(hrkey)
        elif srdict.has_key(srkey):
            srdict.pop(srkey)
        elif grdict.has_key(srkey):
            grdict.pop(srkey)
        csvmfc.writerow(l)
    for k,v in hrdict.iteritems():
        csvmfc.writerow({'state':k[0],'electoral_district_name':k[1],'electoral_district_type':'congressional_district','name':k[2][0],'facebook_url':v,'identifier':'dummy'})
    for k,v in srdict.iteritems():
        csvmfc.writerow({'state':k[0],'electoral_district_name':k[0],'electoral_district_type':'state','name':k[1][0],'facebook_url':v,'identifier':'dummy'})
    for k,v in grdict.iteritems():
        csvmfc.writerow({'state':k[0],'electoral_district_name':k[0],'electoral_district_type':'state','name':k[1][0],'facebook_url':v,'identifier':'dummy'})
    for l in csvpr:
        csvmfc.writerow({'state':'United States','electoral_district_name':'United States','electoral_district_type':'national','name':l['CANDIDATE'],'facebook_url':l['URL'],'identifier':'dummy'})

