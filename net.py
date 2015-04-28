import socket
from multiprocessing import Process, Queue

class NetPipe(object):
    def __init__(self, IP, PORT):
        self.UDP_IP = IP
        self.UDP_PORT = PORT
        self.sock = socket.socket(socket.AF_INET, # Internet
                                  socket.SOCK_DGRAM) # UDP

class SendPipe(NetPipe):
    def __init__(self, IP, PORT):
        NetPipe.__init__(self, IP, PORT)

    def send(self, data):
        self.sock.sendto(data, (self.UDP_IP, self.UDP_PORT))

class RecvPipe(NetPipe):
    def __init__(self, IP, PORT):
        NetPipe.__init__(self, IP, PORT)
        self.sock.bind((self.UDP_IP, self.UDP_PORT))
        self.q = Queue()
        self.listener = Process(target=RecvPipe.__listen, args=(self, ))
        self.listener.deamon = True
    
    def __listen(self):
        while True:
            self.q.put(self.sock.recvfrom(1024))
    
    def start(self):
        self.listener.start()

    def empty(self):
        return self.q.empty()

    def recv(self):
        if self.q.empty():
            return None
        return self.q.get_nowait()