import time
from multiprocessing import Process, Queue
from rpc import RecvPipe

UDP_PORT = 5005

local = RecvPipe("127.0.0.1", UDP_PORT)
local.start()

while True:
    while not local.empty():
        data, addr = local.recv()
        print "<", addr, ">:", data
    print "Nothing to do ..."
    time.sleep(1)
    