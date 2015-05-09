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
from CommitterEventHandler import CommitterEventHandler
from job import Job
from pool import Pool

import sys
import math
from multiprocessing import Queue
from collections import deque
import time
from random import choice
from collections import defaultdict

work = range(10)

def run(IP, PORT):
    # Simulated "event queue"
    eventQueue = deque()
    
    # Process queue used for async rpc system.
    processQ = Queue()
    sessionManager = MasterSessionManager(IP, PORT, processQ)
    rpcManager = RPCManager(sessionManager, processQ)
    containerAllocator = RMContainerAllocator(eventQueue, sessionManager)
    committerEventHandler = CommitterEventHandler(eventQueue)
    printed = False;
    serverList = []
    assignedServers = []
    serverAssignments = defaultdict(list)
    
    pool = Pool()
    
    job = Job(work, pool, rpcManager, eventQueue)
    
    # Simulate Delayed Job init and start.
    eventQueue.append(("JOB_INIT", job))
    eventQueue.append(("JOB_START", job))
    
    while True:
        # Simulate "event delivery"
        containerAllocator.pushNewEvents(eventQueue)
        committerEventHandler.pushNewEvents(eventQueue)
        pool.pushNewEvents(eventQueue)
        eventQueue.clear()
        
        # Simulate async mechanisums
        sessionManager.poll()
        rpcManager.poll()
        containerAllocator.heartbeat()
        committerEventHandler.heartbeat()
        
        # For server failure
        for locator in serverList:
            if (locator not in sessionManager.serverList()):
                eventQueue.append(("JOB_UPDATED_NODES", locator))
        if serverList != sessionManager.serverList():
            print "serverList change"
        serverList = sessionManager.serverList()
    
        # Run tasks
        pool.poll()

        if job.getStatus() == "SUCCEEDED" and not printed:
            print "Job Complete"
            print job
            printed = True

if __name__ == '__main__':
    args = docopt(__doc__)
    print(args)
    work = range(int(args['--taskcount']))
    if (args['--background']):
        pass
    else:
        run(args['<IP>'], int(args['<PORT>']))