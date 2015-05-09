from collections import deque

class CommitterEventHandler(object):
    def __init__(self, eventQueue):
        self.eventsIn = deque()
        self.eventsOut = eventQueue
        # used to simulate async processing
        self.sleepCounter = 10
    
    def heartbeat(self):
        if self.sleepCounter != 0:
            self.sleepCounter = self.sleepCounter - 1
        elif len(self.eventsIn) > 0:
            self.sleepCounter = 10
            eventType, value = self.eventsIn.popleft()
            if eventType == "JOB_SETUP":
                self.handleJobSetup(value)
            if eventType == "JOB_COMMIT":
                self.handleJobCommit(value)
            if eventType == "JOB_ABORT":
                self.handleJobAbort(value)
        
    def pushNewEvents(self, newEvents):
        self.eventsIn += newEvents

    def handleJobSetup(self, value):
        if True:
            self.eventsOut.append(("JOB_SETUP_COMPLETED", value))
        else:
            self.eventsOut.append(("JOB_SETUP_FAILED", value))
    
    def handleJobCommit(self, value):
        if True:
            self.eventsOut.append(("JOB_COMMIT_COMPLETED", value))
        else:
            self.eventsOut.append(("JOB_COMMIT_FAILED", value))
        
    def handleJobAbort(self, value):
        self.eventsOut.append(("JOB_ABORT_COMPLETED", value))