import csv
from multiprocessing import Pool
import requests
from fblogin import logindata
loginurl = 'https://login.facebook.com/login.php'
r = requests.post(loginurl, data=logindata)
r = requests.post(loginurl, data=logindata, cookies=r.cookies)
num_tries = 5
logincookie=r.cookies
def renewlogin():
    global logincookie
    r = requests.post(loginurl, data=logindata)
    r = requests.post(loginurl, data=logindata, cookies=r.cookies)
    logincookie=r.cookies
def get_redirect(line):
    fbid = line['Page ID']
    url = 'http://www.facebook.com/{fbid}'.format(fbid=fbid)
    try:
        for i in range(num_tries):
            site = requests.get(url, cookies=logincookie)
            if site.status_code == 200:
                redirect_url = site.url
                print fbid,redirect_url
                break
        else:
            n = 'COULD NOT LOAD'
            print 'COULD NOT LOAD ' + url
            redirect_url =  'NO DICE'
    except Exception as e:
        print e
        redirect_url = 'ERROR TIME'
    line.update({'url':redirect_url})
    return line

if __name__=='__main__':
    with open('fb/facebookpols.csv') as f, open('fb/facebookpolsurls.csv','w') as g:
        csvr = csv.DictReader(f)
        csvw = csv.DictWriter(g,csvr.fieldnames + ['url'])
        def callb(line):
            print line['url']
            csvw.writerow(line)
        pool = Pool(processes=30)
        for l in csvr:
            #get_redirect(l)
            pool.apply_async(get_redirect, [l],callback=callb)
        pool.close()
        pool.join()
