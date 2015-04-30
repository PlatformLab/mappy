from rpc import RPC

import time

class Task(object):
    def __init__(self, work, rpcManager, containerAllocator):
        self.commitLocator = None
        self.killed = False
        self.work = work
        self.taskAttempts = []
        self.time = None
        self.rpcManager = rpcManager
        self.containerAllocator = containerAllocator
    
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
                taskAttempt.applyRules()
        
        if not self.taskResourcesAvailable():
            self.kill()
            
        if not self.killed and self.commitLocator == None and self.shouldAddAttempt():
            self.taskAttempts.append(TaskAttempt(self.work, self.rpcManager, self.containerAllocator))
        # Need to keep trying unless there is an abort.  Node crashes should trigger reexecution.
        
    def getStatus(self):
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
            taskAttempt.kill(container)
        
    def nodeCrash(self, container):
        if container == self.commitLocator:
            self.commitLocator = None
        for taskAttempt in self.taskAttempts:
            taskAttempt.nodeCrash(container)
    
    def taskResourcesAvailable(self):
        return True 
    
    def __str__(self):
        return "<{0}: {1} {2}>".format(self.work, self.commitLocator, [str(a) for a in self.taskAttempts])

class TaskAttempt(object):
    def __init__(self, work, rpcManager, containerAllocator):
        self.status = "NEW"
        self.container = None
        self.work = work
        self.rpc = None
        self.time = None
        self.rpcManager = rpcManager
        self.containerAllocator = containerAllocator
    
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

    def nodeCrash(self, container):
        if self.container == container:
            self.rpc = None
            self.status = "FAILED"
        
    def getStatus(self):
        if self.status in ("FAILED", "SUCCEEDED") and self.rpc == None:
            return self.status
        return "PENDING"
    
    def applyRules(self):
        if self.status == "NEW":
            # Generating a CONTAINER_REQ "event"
            self.containerAllocator.putNewEvents([("CONTAINER_REQ", (self, None))])
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
                    self.containerAllocator.putNewEvents([("CONTAINER_DEALLOCATE", (self, self.container))])
                else:
                    self.kill()
        elif self.status == "FAILED":
            if self.rpc and self.rpc.status == "complete":
                self.rpc = None
                self.containerAllocator.putNewEvents([("CONTAINER_FAILED", (self, self.container))])
                    
    def __str__(self):
        return "<{0}: {1} {2}>".format(self.work, self.status, self.container)