from rpc import *
from collections import deque

def EXPECT(result):
    if not result:
        raise Exception("NOT GOOD")

def EXPECT_EQ(exp, result):
    if exp != result:
        s = "NOT GOOD, Expected: ", exp, ", Got: ", result
        raise Exception(s)

class TestQueue(deque):
    def put(self, obj):
        self.append(obj)
    def get(self):
        return self.popleft()
    def empty(self):
        return len(self) == 0

class TestSessionManager(object):
    def __init__(self):
        self.sendQ = []

    def poll(self):
        pass
    
    def process(self):
        pass

    def send(self, locator, data):
        self.sendQ.append((locator, data))

    def serverList(self):
        return [("localhost", 5005)]

Q = TestQueue()
SM = TestSessionManager()
RPCM = RPCManager(SM, Q)

# Test mes
EXPECT(Q.empty())
Q.put((("localhost", 5005),json.dumps((1, "msg", None))))
EXPECT(not Q.empty())
RPCM.poll()
EXPECT_EQ(1, len(RPCM.inRPC))
EXPECT(Q.empty())
EXPECT_EQ(1, len(SM.sendQ))

Q.put((("localhost", 5005),json.dumps((1, "msg", None))))
EXPECT(not Q.empty())
RPCM.poll()
EXPECT_EQ(1, len(RPCM.inRPC))
EXPECT(Q.empty())
EXPECT_EQ(2, len(SM.sendQ))

Q.put((("localhost", 5005),json.dumps((2, "msg", None))))
EXPECT(not Q.empty())
RPCM.poll()
EXPECT_EQ(2, len(RPCM.inRPC))
EXPECT(Q.empty())
EXPECT_EQ(3, len(SM.sendQ))


