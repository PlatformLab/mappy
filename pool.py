from collections import deque

class Pool(object):
    def __init__(self):
        self.taskSet = set()
        self.eventsIn = deque()
    
    def poll(self):
        for t in list(self.taskSet):
            t.handleEvents(self.eventsIn)
            t.applyRules()

    def schedule(self, task):
        self.taskSet.add(task)
    
    def deschedule(self, task):
        self.taskSet.remove(task)
            
    def pushNewEvents(self, newEvents):
        self.eventsIn += newEvents