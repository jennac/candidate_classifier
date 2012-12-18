from multiprocessing import Lock

class Queue():
    def __init__(self, object_list):
        self.objects = object_list
        #self.objects = dict((i,(object_list[i],Lock())) for i in range(len(object_list)))
        self.lock = Lock()

    def get(self):
        self.lock.acquire()
        o = self.objects.pop()
        print 'len objects: {l}'.format(l=len(self.objects))
        if len(self.objects) > 0:
            self.lock.release()
        return o

    def put(self, o):
        self.objects.append(o)
        if len(self.objects) == 1:
            self.lock.release()
