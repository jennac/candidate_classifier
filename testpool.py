from multiprocessing import Pool
from dummy import retval
pool = Pool(processes=4)

def callb(ni):
    print ni[0]
    print ni[1]

a = pool.apply_async(func=retval, callback=callb)
pool.close()
pool.join()
print a
