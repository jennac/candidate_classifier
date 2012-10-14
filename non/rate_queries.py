import csv, numpy, itertools
with open('webpage_ssv.csv') as f:
    csvr = csv.reader(f)
    tests = []
    totals = [0]*4
    csvr.next()
    x = 0
    for l in csvr:
        a = map(int,eval(l[2]))
        tests.append(a)
        totals = [totals[i]+a[i] for i in range(len(totals))]
        x += 1
    print x
    zip_totals = numpy.array(map(numpy.array,zip(*tests)))
    for i in range(1,len(totals)+1):
        maximum = 0
        for p in itertools.permutations(range(len(totals)),i):
            m = sum(map(lambda x: x > 0,sum(zip_totals[list(p)])))
            if m > maximum:
                maximum = m
                max_perm = p
        print 'max perm for {i} is {p} hits {m}'.format(i=i,p=max_perm,m=maximum)

    print totals
