from rpc import RPC

import time
from collections import deque

class Job(object):
    def __init__(self, workList, pool, rpcManager, eventQueue):
        self.status = "NEW"
        self.scheduled = False
        self.setup = False
        self.killed = False
        self.workList = workList
        self.taskList = []
        self.pool = pool
        self.rpcManager = rpcManager
        self.eventQueue = eventQueue
        self.eventsIn = deque()
        # schedule itself to run
        self.pool.schedule(self)
    
    def applyRules(self):
        if self.status == "NEW":
            pass
        elif self.status == "INITED":
            pass 
        elif self.status == "SETUP":
            pass
        elif self.status == "RUNNING":
            allDone = True
            for task in self.taskList:
                if task.getStatus() == "KILLED_OR_FAILED":
                    self.status = "FAILED"
                    allDone = False
                    break
                if task.getStatus() == "RUNNING":
                    allDone = False
                    break
            if allDone:
                self.eventQueue.append(("JOB_COMMIT", self))
                self.status = "COMMITTING"
        elif self.status == "COMMITTING":
            pass
        elif self.status == "SUCCEEDED":
            pass
        elif self.status == "FAILED":
            for task in self.taskList:
                task.kill()
                
        if self.getStatus() in ("SUCCEEDED", "KILLED_OR_FAILED"):
            self.pool.deschedule(self)
    
    def handleEvents(self, newEvents):
        self.eventsIn += newEvents

        # Turn events into state changes
        for eventType, value in list(self.eventsIn):
            # JobEventType Events
            if eventType == "JOB_INIT":
                if self.status == "NEW":
                    self.taskList =[Task(w, self.pool, self.rpcManager, self.eventQueue) for w in self.workList]
                    self.status = "INITED"
                    print "INITED"
            if eventType == "JOB_START":
                if self.status == "INITED":
                    self.eventQueue.append(("JOB_SETUP", self))
                    self.status = "SETUP"
                    print "SETUP"
            if eventType == "JOB_SETUP_COMPLETED":
                if self.status == "SETUP" and value == self:
                    self.setup = True
                    for task in self.taskList:
                        task.scheduled = True
                    self.status = "RUNNING"
                    print "RUNNING"
            if eventType == "JOB_SETUP_FAILED":
                if self.status == "SETUP" and value == self:
                    self.status = "FAILED"
            if eventType == "JOB_COMMIT_COMPLETED":
                if self.status == "COMMITTING" and value == self:
                    self.status = "SUCCEEDED"
            if eventType == "JOB_COMMIT_FAILED":
                if self.status == "COMMITTING" and value == self:
                    self.status = "FAILED"
            if eventType == "JOB_KILL":
                self.eventQueue.append(("JOB_ABORT", self))
                self.status = "FAILED"
            if eventType == "JOB_ABORT_COMPLETED":
                if self.status == "FAILED" and value == self:
                    self.setup = False
            if eventType == "JOB_UPDATED_NODES":
                for task in self.taskList:
                    task.nodeCrash(value)
            if eventType == "JOB_DIAGNOSTIC_UPDATE":
                # Do some update
                pass
        self.eventsIn.clear()
    
    def pushNewEvents(self, newEvents):
        self.eventsIn += newEvents
    
    def getStatus(self):
        if self.setup:
            for task in self.taskList:
                if task.getStatus() != "SUCCEEDED":
                    return "RUNNING"
            return "SUCCEEDED"
        else:
            for task in self.taskList:
                if task.getStatus() != "KILLED_OR_FAILED":
                    return "RUNNING"
            return "FAILED"
        
    def __str__(self):
        s = ""
        for task in self.taskList:
            s = s + str(task) + "\n"
        return s

class Task(object):
    def __init__(self, work, pool, rpcManager, eventQueue):
        self.commitLocator = None
        self.scheduled = False
        self.killed = False
        self.work = work
        self.taskAttempts = []
        self.time = None
        self.pool = pool
        self.rpcManager = rpcManager
        self.eventQueue = eventQueue
        self.eventsIn = deque()
        # schedule itself to run
        self.pool.schedule(self)
    
    def applyRules(self):
        # Make sure the subtasks are run
        for taskAttempt in list(self.taskAttempts):
            if taskAttempt.getStatus() == "FAILED":
                self.taskAttempts.remove(taskAttempt)
            elif taskAttempt.getStatus() == "SUCCEEDED":
                if self.commitLocator == None:
                    self.commitLocator = taskAttempt.container
                self.taskAttempts.remove(taskAttempt)
            else:
                if self.commitLocator != None:
                    taskAttempt.kill()
        
        if not self.taskResourcesAvailable():
            self.kill()
            
        if self.scheduled and not self.killed and self.commitLocator == None and self.shouldAddAttempt():
            self.taskAttempts.append(TaskAttempt(self.work, self.pool, self.rpcManager, self.eventQueue))
            
        if self.getStatus() in ("SUCCEEDED", "KILLED_OR_FAILED"):
            self.pool.deschedule(self)
            
    def handleEvents(self, newEvents):
        pass
        
    def getStatus(self):
        if not self.scheduled:
            return "NEW"

        if len(self.taskAttempts) == 0:
            if not self.killed:
                if self.commitLocator != None:
                    return "SUCCEEDED"
                else:
                    return "RUNNING"
            else:
                return "KILLED_OR_FAILED"
        else:
            return "RUNNING"
        
    # Some policy for whether an attempt should be issued.
    # Same affect as if an event T_ADD_SPEC_ATTEMPT was generated
    def shouldAddAttempt(self):
        if len(self.taskAttempts) == 0:
            return True
            
    def kill(self):
        self.killed = True
        self.commitLocator = None
        for taskAttempt in self.taskAttempts:
            taskAttempt.kill()
        self.pool.schedule(self)
        
    def nodeCrash(self, container):
        if container == self.commitLocator:
            self.commitLocator = None
        for taskAttempt in self.taskAttempts:
            taskAttempt.nodeCrash(container)
        self.pool.schedule(self)
    
    def taskResourcesAvailable(self):
        return True 
    
    def __str__(self):
        return "<{0}: {1} {2}>".format(self.work, self.getStatus(), self.commitLocator)

class TaskAttempt(object):
    def __init__(self, work, pool, rpcManager, eventQueue):
        self.status = "NEW"
        self.container = None
        self.work = work
        self.rpc = None
        self.time = None
        self.pool = pool
        self.rpcManager = rpcManager
        self.eventQueue = eventQueue
        self.eventsIn = deque()
        # schedule itself to run
        self.pool.schedule(self)
        
    def applyRules(self):
        if self.status == "NEW":
            # Generating a CONTAINER_REQ "event"
            self.eventQueue.append(("CONTAINER_REQ", (self, None)))
            self.status = "UNASSIGNED"
        elif self.status == "UNASSIGNED":
            if self.container != None:
                self.status = "ASSIGNED"
        elif self.status == "ASSIGNED":
            self.rpc = RPC(self.container, None, ("LAUNCH", self.work))
            self.rpcManager.send(self.rpc)
            self.status = "RUNNING"
            self.time = time.time()
        elif self.status == "RUNNING":
            if self.rpc.status == "complete":
                if self.rpc.reply != "failed":
                    self.rpc = RPC(self.container, None, ("COMMIT", self.work))
                    self.rpcManager.send(self.rpc)
                    self.status = "COMMIT_PENDING"
                else:
                    self.kill()
        elif self.status == "COMMIT_PENDING":
            if self.rpc.status == "complete":
                if self.rpc.reply != "failed":
                    self.status = "SUCCEEDED"
                    self.rpc = None
                    self.eventQueue.append(("CONTAINER_DEALLOCATE", (self, self.container)))
                else:
                    self.kill()
        elif self.status == "FAILED":
            if self.rpc and self.rpc.status == "complete":
                self.rpc = None
                self.eventQueue.append(("CONTAINER_FAILED", (self, self.container)))
        
        if self.getStatus() in ("FAILED", "SUCCEEDED"):
            self.pool.deschedule(self)
                
    def handleEvents(self, newEvents):
        self.eventsIn += newEvents

        # Turn events into state changes
        for eventType, value in list(self.eventsIn):
            # TaskAttemptEventType Events
            if eventType == "TA_ASSIGNED":
                taskAttempt, container = value
                taskAttempt.assignContainer(container)
            if eventType == "TA_KILL":
                taskAttempt, container = value
                taskAttempt.kill()
        self.eventsIn.clear()
    
    def assignContainer(self, container):
        if self.container == None:
            print "Container Assigned: " + str(container) + " to " + str(self.work)
            self.container = container
    
    def kill(self):
        # performs CONTAINER_REMOTE_CLEANUP
        if self.status == "SUCCEEDED":
            pass
        else:
            if self.container != None:
                self.rpc = RPC(self.container, None, ("CONTAINER_REMOTE_CLEANUP", self.work))
                self.rpcManager.send(self.rpc)
            self.status = "FAILED"
        self.pool.schedule(self)

    def nodeCrash(self, container):
        if self.container == container:
            self.rpc = None
            self.status = "FAILED"
        self.pool.schedule(self)
        
    def getStatus(self):
        if self.status in ("FAILED", "SUCCEEDED") and self.rpc == None:
            return self.status
        return "PENDING"
                    
    def __str__(self):
        return "<{0}: {1} {2}>".format(self.work, self.status, self.container)