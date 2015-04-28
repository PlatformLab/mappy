from net import *
import time
import json
from random import randint

RETRY = 0.5
WORRY = 8
TIMEOUT = 10


class Session(object):
    def __init__(self, IP, PORT, ID, manager):
        self.locator = (IP, PORT, ID)
        self.manager = manager
        self.sender = SendPipe(IP, PORT)
        self.txTime = 0
        self.rxTime = 0
    
    def poll(self):
        Time = time.time()
        rxTime = Time - self.rxTime
        txTime = Time - self.txTime
        if (rxTime > WORRY) and (txTime > RETRY):
            self.send("ping")

    def event(self):
        self.rxTime = time.time()
    
    def send(self, data):
        self.txTime = time.time()
        packet = json.dumps((self.manager.locator, data))
        print "TX: ", packet
        self.sender.send(packet)

class SessionManager(object):
    def __init__(self, IP, PORT, processQ):
        self.locator = (IP, PORT, randint(0,9999))
        self.receiver = RecvPipe(IP, PORT)
        self.receiver.start()
        self.sessions = {}
        self.processQ = processQ
        self.nextSessionID = 1

    def poll(self):
        self.process()
        for loc in self.sessions.keys():
            self.sessions[loc].poll()
        # Kill old sessions
        for loc in self.sessions.keys():
            if time.time() - self.sessions[loc].rxTime > TIMEOUT:
                self.sessions.pop(loc)
    
    def process(self):
        while not self.receiver.empty():
            packet = self.receiver.recv()[0]
            locator, data = json.loads(packet)
            locator = tuple(locator)
            if locator not in self.serverList():
                self.sessions[locator] = Session(locator[0],
                                                 locator[1], 
                                                 locator[2],
                                                 self)
            self.sessions[locator].event()
            if data == "ping":
                print "RX: ", locator, data
                self.sessions[locator].send("pong")
            elif data == "pong":
                print "RX: ", locator, data
            else:
                print "RX: ", locator, data
                self.processQ.put((locator, data))

    def send(self, locator, data):
        if locator in self.sessions.keys():
            self.sessions[locator].send(data)

    def serverList(self):
        return self.sessions.keys()

class MasterSessionManager(SessionManager):
    pass

class WorkerSessionManager(SessionManager):
    def __init__(self, IP, PORT, mIP, mPORT, processQ):
        SessionManager.__init__(self, IP, PORT, processQ)
        self.defaultMasterSession = Session(mIP, mPORT, 0, self)
    
    def poll(self):
        if len(self.sessions) == 0:
            self.sessions[self.defaultMasterSession.locator] = self.defaultMasterSession
        elif len(self.sessions) > 1 and self.defaultMasterSession.locator in self.sessions:
            del self.sessions[self.defaultMasterSession.locator]
        SessionManager.poll(self)