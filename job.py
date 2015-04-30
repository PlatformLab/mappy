from rpc import RPC

import time

class Task(object):
    def __init__(self, work):
        self.status = "unassigned"
        self.work = work
        self.taskAttempts = []
        self.time = None
    
    def applyRules(self):
        pass
    
    def assign(self, locator, rpc):
        self.assignment.append(locator)
        self.status = "assigned"
        self.rpcs.append(rpc)
        self.time = time.time()
    
    def poll(self, assignedServers):
        for rpc in self.rpcs:
            if rpc.status == "complete":
                self.complete(rpc.locator)
                assignedServers.remove(rpc.locator)
                self.rpcs.remove(rpc)
            elif rpc.status == "failed":
                self.failure(rpc.locator)
                self.rpcs.remove(rpc)
    
    def complete(self, locator):
        if self.status != "complete":
            self.status = "complete"
            self.assignment = []
        self.assignment.append(locator)

    def failure(self, locator):
        if locator in self.assignment:
            self.assignment.remove(locator)
            print self.assignment
        if len(self.assignment) == 0:
            self.status = "unassigned"
            print "TASK Failed"
    
    def __str__(self):
        return "<{0}: {1} {2}>".format(self.work, self.status, self.assignment)

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
    
    def abort(self):
        # performs CONTAINER_REMOTE_CLEANUP
        if self.status == "SUCCEEDED":
            pass
        else:
            if self.container != None:
                self.rpc = RPC(self.container, None, ("ABORT", self.work))
                self.rpcManager.send(self.rpc)
            self.status = "FAILED"

    def nodeCrash(self, container):
        if self.container == container:
            self.rpc = None
            self.status = "FAILED"
        
    def getStatus(self):
        if self.status in ("FAILED", "SUCCEEDED") and self.rpc != None:
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
                    self.abort()
        elif self.status == "COMMIT_PENDING":
            if self.rpc.status == "complete":
                if self.rpc.reply != "failed":
                    self.status = "SUCCEEDED"
                    self.containerAllocator.putNewEvents([("CONTAINER_DEALLOCATE", (self, self.container))])
                else:
                    self.abort()
        elif self.status == "FAILED":
            if self.rpc and self.rpc.status == "complete":
                self.rpc = None
                self.containerAllocator.putNewEvents([("CONTAINER_FAILED", (self, self.container))])
                    
    def __str__(self):
        return "<{0}: {1} {2}>".format(self.work, self.status, self.container)