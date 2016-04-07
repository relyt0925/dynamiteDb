#!/usr/bin/python           # This is client.py file
import time
import socket               # Import socket module
import json
import sys
from thread import *
from threading import Thread, Lock

mutex = Lock()

HOST = '24.72.242.230'
PORT = 12359               # Reserve a port for your service.

data={}

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
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

testdata = {}
testdata['KEY'] = 'dummy_key'
testdata['METHOD'] = 'GET'
testdata['VALUE'] = 'dbnode2_val'
testdata['TIMESTAMP'] = '2015-04-06 02:18:00.248554'
testdata['VECTOR_CLOCK'] = {'YaksPC/192.168.1.1': 2, 'A' : 8 , 'B' : 3, 'C' : 8, 'Z' : 23}

def clientthread(conn):
    data1 = json.loads(conn.recv(1024))
    data.update(data1)
    print data


    
    if(data['METHOD']=='GET'):
        print testdata
        conn.send(json.dumps(testdata))
    else:
        mutex.acquire()
        testdata['VALUE']=data['VALUE']
        print testdata
        conn.send('OK')
        mutex.release()    


while 1:
    #wait to accept a connection - blocking call
    conn, addr = s.accept()
    print 'Connected with ' + addr[0] + ':' + str(addr[1])
    start_new_thread(clientthread ,(conn,))

s.close()                  # Close the socket when done