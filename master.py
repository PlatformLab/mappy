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

def chooseServer(serverList, serverAssignments, assignedServers, task):
    # Chose the server with the least assignments that doesn't alread have it
    # assigned
    server = None
    for locator in serverList:
        if (locator not in assignedServers and
            task not in serverAssignments[locator] and
            (server == None or
             len(serverAssignments[locator]) < len(serverAssignments[server]))):
            server = locator
    return server
    
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
        taskList.append(Task(w))
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
                    task.failure(locator)
        serverList = sessionManager.serverList()
        for locator in assignedServers:
            if locator not in serverList:
                assignedServers.remove(locator)
    
        # Check for complete of failed tasks
        for task in taskList:
            task.poll(assignedServers)

        for task in taskList:
            # Assign Work or
            # Reassign work if it seems to be taking a while
            if (len(serverList) > len(assignedServers) and
                (task.status == "unassigned" or 
                task.status != "complete" and time.time() - task.time > 5 * 1.1)):
                assignTask(rpcManager, serverList, serverAssignments, assignedServers, task)

        # Hope we are all done
        if allDone == False:
            allDone = True
            for task in taskList:
                if task.status != "complete":
                    allDone = False
                    break
            if allDone:
                print "ALL DONE!"
                for task in taskList:
                    print task
        else:
            for task in taskList:
                if task.status != "complete":
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