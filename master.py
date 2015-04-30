#!/usr/bin/env python

"""Master Map Service.

Usage:
    master.py [-b] [-t <tc>] <IP> <PORT>

Options:
  -h --help                 Show this screen.
  -b --background           Run service in background.
  -t --taskcount=<tc>       Number of tasks to be performed [default: 10].
"""
from docopt import docopt
from rpc import RPCManager, RPC
from session import MasterSessionManager
import math
from multiprocessing import Queue
import time
from random import choice
from collections import defaultdict

work = range(10)

class Task(object):
    def __init__(self, work):
        self.status = "unassigned"
        self.work = work
        self.assignment = []
        self.rpcs = []
        self.time = None
    
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
    def __init__(self, work, rpcManager):
        self.status = "NEW"
        self.container = None
        self.work = work
        self.rpc = None
        self.time = None
        self.rpcManager = rpcManager

    def assignContainer(self, container):
        if self.container == None:
            print "Container Assigned: " + str(container) + " to " + str(self.work)
            self.container = container
    
    def abort(self):
        # performs CONTAINER_REMOTE_CLEANUP
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
    
    def applyRules(self, assignedServers):
        if self.container:
            if self.status == "NEW":
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
                        assignedServers.remove(self.container)
                    else:
                        self.abort()
            elif self.status == "FAILED":
                if self.rpc and self.rpc.status == "complete":
                    self.rpc = None
                    assignedServers.remove(self.container)
                    
    def __str__(self):
        return "<{0}: {1} {2}>".format(self.work, self.status, self.container)

# choose a container based on some objective function
def chooseContainer(serverList, assignedServers, task):
    container = None
    for locator in serverList:
        if locator not in assignedServers:
            container = locator
            break
    if container != None:
        print "Container Chosen for work " + str(task.work)
        assignedServers.append(container)
        task.assignContainer(container)
    
def assignTask(rpcManager, serverList, serverAssignments, assignedServers, task):
    locator = chooseServer(serverList, serverAssignments, assignedServers, task)
    if locator != None:
        rpc = RPC(locator, None, task.work)
        task.assign(locator, rpc)
        serverAssignments[locator].append(task)
        assignedServers.append(locator)
        rpcManager.send(rpc)

def run(IP, PORT):
    processQ = Queue()
    sessionManager = MasterSessionManager(IP, PORT, processQ)
    rpcManager = RPCManager(sessionManager, processQ)
    taskList = []
    # Make tasks
    for w in work:
        taskList.append(TaskAttempt(w, rpcManager))
    allDone = False;
    serverList = []
    assignedServers = []
    serverAssignments = defaultdict(list)
    while True:
        sessionManager.poll()
        rpcManager.poll()
        
        # For server failure
        for locator in serverList:
            if (locator not in sessionManager.serverList()):
                for task in taskList:
                    task.nodeCrash(locator)
        if serverList != sessionManager.serverList():
            print "serverList change"
        serverList = sessionManager.serverList()
        for locator in assignedServers:
            if locator not in serverList:
                assignedServers.remove(locator)
    
        # Check for complete of failed tasks
        for task in taskList:
            task.applyRules(assignedServers)

        for task in taskList:
            if task.container == None:
                chooseContainer(serverList, assignedServers, task)

        # Hope we are all done
        if allDone == False:
            allDone = True
            for task in taskList:
                if task.getStatus() != "SUCCEEDED":
                    allDone = False
                    break
            if allDone:
                print "ALL DONE!"
                for task in taskList:
                    print task
        else:
            for task in taskList:
                if task.getStatus() != "SUCCEEDED":
                    allDone = False
                    break
            if not allDone:
                print "Failure after complete"
        

if __name__ == '__main__':
    args = docopt(__doc__)
    print(args)
    work = range(int(args['--taskcount']))
    if (args['--background']):
        pass
    else:
        run(args['<IP>'], int(args['<PORT>']))