from ersatzpg import ersatz
import frame_pages_conf
import os, re, subprocess, time
os.chdir('basic')
webpages = [d for d in os.listdir('.') if re.match(r'webpages\d\d+',d)]
webpages.sort()
for w in webpages:
    print w
    for k,v in frame_pages_conf.ERSATZPG_CONFIG['tables'].iteritems():
        v['filename'] = '/home/gaertner/code/candclass/basic/{webpages}'.format(webpages=w)
    t = time.time()
    ersatz.new_process_copies(frame_pages_conf)
    print "Processed {w} in {t} seconds".format(w=w,t=(time.time()-t))

    pipe = subprocess.Popen(['mv',w,'done'+w[-2:]],stdin=subprocess.PIPE)
    pipe.wait()

