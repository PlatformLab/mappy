import socket
from rpc import SendPipe

UDP_IP = "127.0.0.1"
UDP_PORT = 5005
MESSAGE = "Hello, World!"

print "UDP target IP:", UDP_IP
print "UDP target port:", UDP_PORT
print "message:", MESSAGE

sock = SendPipe(UDP_IP, UDP_PORT)
sock1 = SendPipe(UDP_IP, UDP_PORT)
sock2 = SendPipe(UDP_IP, UDP_PORT)
sock.send(MESSAGE)
