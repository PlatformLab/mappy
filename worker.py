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
    while True:
        sessionManager.poll()
        rpcManager.poll()
        if working == None:
            for rpc in rpcManager.inRPC.values():
                if rpc.status == "pending":
                    rpc.temp = time.time() + 5.0 + RAND * gauss(0.0, 1.0)
                    rpc.status = "working"
                    working = rpc
                    print "Working Assigned: ", working.msg, working.locator
                    break
        else:
            if time.time() > working.temp:
                working.reply = working.msg
                working.status = "send"
                print "Work Finished: ", working.msg, working.locator
                working = None
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