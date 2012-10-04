from ersatzpg import ersatz
import non_pages_conf
import os, re, subprocess
webpages = [d for d in os.listdir('.') if re.match(r'webpages_ex\d\d+',d)]
webpages.sort()
for w in webpages:
    print w
    for k,v in non_pages_conf.ERSATZPG_CONFIG['tables'].iteritems():
        v['filename'] = '/home/gaertner/code/candclass/{webpages}'.format(webpages=w)
    ersatz.new_process_copies(non_pages_conf)

    pipe = subprocess.Popen(['mv',w,'done_ex'+w[-2:]],stdin=subprocess.PIPE)
    pipe.wait()
