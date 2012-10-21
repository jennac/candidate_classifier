import requests
import json

def get_results(query, custom_engine_id, key='AIzaSyCdHlGJuMzGBH9hNsEMObffDIkzJ44EQhA', sleep_limit=None, print_query=False):
    query_url = u'https://www.googleapis.com/customsearch/v1?cx={custom_engine_id}&key={key}&hl=en&q={query}'.format(query=query, custom_engine_id=custom_engine_id, key=key)
    if print_query:
        print query
    n = 1
    while True:
        site = requests.get(query_url)
        if site.status_code == 200:
            return json.loads(site.text)
        else:
            if site.status_code == 403 or site.status_code == 503:
                if sleep_limit and n > sleep_limit:
                    raise Exception(site.status_code + ' ' + site.reason)
                print 'sleeping'
                time.sleep(n + random.randint(1,1000)/1000.)
                n = n*2
            else:
                raise Exception(site.status_code + ' ' + site.reason)

