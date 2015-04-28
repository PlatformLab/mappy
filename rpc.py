import time
import json

class RPC(object):
    def __init__(self, locator, rcpId, msg):
        self.locator = locator
        self.id = rcpId
        self.msg = msg
        self.reply = None
        self.temp = None
        self.status = "pending"
        self.time = time.time()
    
    def __str__(self):
        s = "<" , self.locator, ", "
        s = s, self.id, ", ",
        s = s, self.msg, ", ",
        s = s, self.reply, ", ",
        s = s, self.status, ">"
        return s

class RPCManager(object):
    def __init__(self, sessionManager, inQ):
        self.inQ = inQ
        self.sessionManager = sessionManager
        self.inRPC = {}
        self.outRPC = {}
        self.counter = 0
    
    def poll(self):
        # Get incomming RPC
        while not self.inQ.empty():
            locator, datastr = self.inQ.get()
            rpcId, kind, data = json.loads(datastr)
            # 0 is sender
            if kind == "msg":
                if (locator, rpcId) not in self.inRPC.keys():
                    self.inRPC[(locator, rpcId)] = RPC(locator, rpcId, data)
                rpc = self.inRPC[(locator, rpcId)]
                if rpc.status == "complete":
                    # resend reply
                    self.sessionManager.send(rpc.locator,
                                             json.dumps((rpc.id,
                                                         "reply",
                                                         rpc.reply)))
                    print "RPC Replied"
                else:
                    # send ack
                    self.sessionManager.send(locator,
                                             json.dumps((rpcId, "ack", None)))
            elif kind == "reply":
                if (locator, rpcId) in self.outRPC.keys():
                    self.outRPC[(locator, rpcId)].reply = data
                    self.outRPC[(locator, rpcId)].status = "complete"
                    print "RPC Complete"
            elif kind == "ack":
                if (locator, rpcId) in self.outRPC.keys():
                    rpc = self.outRPC[(locator, rpcId)]
                    if rpc.status == "pending":
                        rpc.status = "acked"
                if (locator, rpcId) in self.inRPC.keys():
                    del self.inRPC[(locator, rpcId)]

        # send out rpc reply
        for rpc in self.inRPC.values():
            if rpc.status == "send":
                self.sessionManager.send(rpc.locator,
                                         json.dumps((rpc.id,
                                                     "reply",
                                                     rpc.reply)))
                rpc.status = "complete"
                rpc.time = time.time()
                print "RPC Replied"
        # check on outstanding RPCs
        for key in self.outRPC.keys():
            rpc = self.outRPC[key]
            # mark rpc failed if server dies
            if rpc.locator not in self.sessionManager.serverList():
                if rpc.status != "failed":
                    rpc.status = "failed"
                    print "RPC Failed"
            # resend RPC is no ack is recived in time
            if rpc.status == "pending" and (time.time() - rpc.time) > 0.25:
                self.send(rpc)
                
            

    def send(self, rpc):
        rpc.id = "{0}:{1}:{2}".format(self.sessionManager.locator,
                                      rpc.locator,
                                      self.counter)
        self.counter += 1
        self.outRPC[(rpc.locator, rpc.id)] = rpc
        self.sessionManager.send(rpc.locator, json.dumps((rpc.id,
                                                          "msg",
                                                          rpc.msg)))
        rpc.time = time.time()
        print "RPC Sent"
