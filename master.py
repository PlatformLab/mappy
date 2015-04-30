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
from rpc import RPCManager
from session import MasterSessionManager
from RMContainerAllocator import RMContainerAllocator
from job import TaskAttempt

import math
from multiprocessing import Queue
import time
from random import choice
from collections import defaultdict

work = range(10)

def run(IP, PORT):
    processQ = Queue()
    sessionManager = MasterSessionManager(IP, PORT, processQ)
    rpcManager = RPCManager(sessionManager, processQ)
    containerAllocator = RMContainerAllocator(sessionManager)
    taskList = []
    # Make tasks
    for w in work:
        taskList.append(TaskAttempt(w, rpcManager, containerAllocator))
    allDone = False;
    serverList = []
    assignedServers = []
    serverAssignments = defaultdict(list)
    while True:
        sessionManager.poll()
        rpcManager.poll()
        containerAllocator.heartbeat()
        
        # Turn events into state changes
        for eventType, value in containerAllocator.getNewEvents():
            if eventType == "TA_ASSIGNED":
                taskAttempt, container = value
                taskAttempt.assignContainer(container)
        
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
            task.applyRules()

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