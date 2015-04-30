#!/usr/bin/env python

"""Map Service Worker.

Usage:
    worker.py [-br] [(-d <dr> -t <ttl>)] <IP> <PORT> <MASTER_IP> <MASTER_PORT>

Options:
  -h --help                 Show this screen.
  -b --background           Run service in background.
  -r --random               Randomize completion time.
  -d --die=<dr>             Probability that the worker will die [default: 0.0].
  -t --timeToLive=<ttl>     Average time worker lives before death in tasks.
"""
from docopt import docopt
from rpc import RPCManager, RPC
from session import WorkerSessionManager
import time
import daemon
from multiprocessing import Queue
from random import gauss, random
import sys

RAND = 0
DIE = False
TTL = 0


def run(IP, PORT, mIP, mPORT):
    processQ = Queue()
    sessionManager = WorkerSessionManager(IP, PORT, mIP, mPORT, processQ)
    rpcManager = RPCManager(sessionManager, processQ)
    working = None
    state = "IDLE";
    doneTime = 0
    while True:
        sessionManager.poll()
        rpcManager.poll()
        
        # Convert incoming RPCs (events) into state changes
        for rpc in rpcManager.inRPC.values():
            if rpc.status == "pending":
                rpcType, payload = rpc.msg
                if rpcType == "LAUNCH":
                    if state == "IDLE":
                        state = "RUNNING"
                        working = rpc
                        rpc.status = "working"
                        continue
                elif rpcType == "COMMIT":
                    if state == "COMPLETE":
                        state = "COMMITTING"
                        working = rpc
                        rpc.status = "working"
                        continue
                elif rpcType == "ABORT":
                    state = "CLEANUP"
                    if working != None:
                        working.reply = "failed"
                        working.status = "send"
                    working = rpc
                    rpc.status = "working"
                    continue
                elif rpcType == "DIE":
                    sys.exit(0)
                
                rpc.reply = "failed"
                rpc.status = "send"
                    
        # Rules engine
        if state == "IDLE":
            pass
        elif state == "RUNNING":
            # simulate random completion time
            if doneTime == 0:
                doneTime = time.time() + 5.0 + RAND * gauss(0.0, 1.0)
            if time.time() > doneTime:
                working.reply = working.msg
                working.status = "send"
                print "Work Finished: ", working.msg, working.locator
                working = None
                doneTime = 0
                state = "COMPLETE"
        elif state == "COMPLETE":
            pass
        elif state == "COMMITTING":
            # simulate random commit time
            if doneTime == 0:
                doneTime = time.time() + 2.0 + RAND * gauss(0.0, 1.0)
            if time.time() > doneTime:
                print "Work Committed: ", working.msg, working.locator
                doneTime = 0
                state = "CLEANUP"
        elif state == "CLEANUP":
            # simulate random cleaup time
            if doneTime == 0:
                doneTime = time.time() + 1.0 + RAND * gauss(0.0, 1.0)
            if time.time() > doneTime:
                working.reply = working.msg
                working.status = "send"
                print "Container Clean: ", working.msg, working.locator
                working = None
                doneTime = 0
                state = "IDLE"

        # Kill failed RPCs
        for key in rpcManager.outRPC.keys():
            if rpcManager.outRPC[key].status == "failed":
                del rpcManager.outRPC[key]
        
        if DIE and time.time() > TTL:
            sys.exit(0)
    

if __name__ == '__main__':
    args = docopt(__doc__)
    print(args)
    RAND = int(args['--random'])
    if random() < float(args['--die']):
        DIE = True
        TTL = time.time() + 5.0 * gauss(float(args['--timeToLive']),
                                        float(args['--timeToLive']) / 2)
    if (args['--background']):
        with daemon.DaemonContext():
            run(args['<IP>'], int(args['<PORT>']),
                args['<MASTER_IP>'], int(args['<MASTER_PORT>']))
        print "Done"
    else:
        run(args['<IP>'], int(args['<PORT>']),
            args['<MASTER_IP>'], int(args['<MASTER_PORT>']))