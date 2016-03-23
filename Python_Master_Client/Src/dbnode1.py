#!/usr/bin/python           # This is client.py file
import time
import socket               # Import socket module
import json

# s = socket.socket()         # Create a socket object
# host = socket.gethostname() # Get local machine name
HOST = '24.72.242.230'
PORT = 12348               # Reserve a port for your service.

data={}

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
print 'Socket created'
 
#Bind socket to local host and port
try:
    s.bind((HOST, PORT))
except socket.error as msg:
    print 'Bind failed. Error Code : ' + str(msg[0]) + ' Message ' + msg[1]
    sys.exit()
     
print 'Socket bind complete'
 
#Start listening on socket
s.listen(10)
print 'Socket now listening'

while 1:
    #wait to accept a connection - blocking call
    conn, addr = s.accept()
    print 'Connected with ' + addr[0] + ':' + str(addr[1])
    data1 = json.loads(conn.recv(1024))
    data.update(data1)
    print data
    print data1.keys()[0]
    it=0
    for find in data.keys():
        if find == data1.keys()[0]:
    	   conn.send(json.dumps(data[data.keys()[it]][0]))
        it=it+1
 
s.close()                  # Close the socket when done