#!/usr/bin/python           # This is client.py file
import time
import socket               # Import socket module
import json
import sys

# s = socket.socket()         # Create a socket object
# host = socket.gethostname() # Get local machine name
# HOST = '24.72.242.230'
HOST = '192.168.56.1'
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

    testdata = {}
    testdata['KEY'] = 'dummy_key'
    testdata['METHOD'] = 'GET'
    testdata['VALUE'] = 'dbnode1_val'
    testdata['TIMESTAMP'] = 100
    testdata['VECTOR_CLOCK'] = ['YaksPC/192.168.1.1', { 'A' : 9 , 'B' : 1, 'C' : 9, 'Z' : 12, 'X' : 89}]

    print testdata
    conn.send(json.dumps(testdata))

    # print data
    # print data1.keys()[0]
    # it=0
    # for find in data.keys():
    #     if find == data1.keys()[0]:
    # 	   conn.send(json.dumps(data[data.keys()[it]][0]))
    #     it=it+1
 
s.close()                  # Close the socket when done