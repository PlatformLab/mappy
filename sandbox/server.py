import select
import socket

server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
server_socket.bind(('', 8888))
server_socket.listen(5)
print "Listening on port 8888"

read_list = [server_socket]
while True:
    readable, writable, errored = select.select(read_list, [], [])
    for s in readable:
        if s is server_socket:
            client_socket, address = server_socket.accept()
            read_list.append(client_socket)
            print "Connection from", address
        else:
            data = s.recv(1024)
            if data:
                print address, data
                s.send(data)
            else:
                s.close()
                read_list.remove(s)
                pass