#!/usr/bin/python           # This is client.py file

import socket               # Import socket module
import json
import hashlib
import datetime

def testGet():
    s = socket.socket()         # Create a socket object
    host = 'localhost' # Get local machine name
    port = 13000                # Reserve a port for your service.
    data = {}
    keyy='IROCKKKKKKKK'
    kevVal=hashlib.sha256(keyy).hexdigest()
    data['KEY'] = kevVal
    data['METHOD'] = 'GET'
    data['VALUE'] = ' '
    print str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"))
    data['TIMESTAMP']=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
    vector_clock = {}
    vector_clock['A'] = '1'
    vector_clock['B'] = '2'
    vector_clock['C'] = '3'
    data['VECTOR_CLOCK'] = vector_clock
    json_data = json.dumps(data)
    s.connect((host, port))
    s.send(b''+json_data+'\n')
    print s.recv(1024)
    s.close()                     # Close the socket when done
    
def testPut():
    s = socket.socket()         # Create a socket object
    host = 'localhost' # Get local machine name
    port = 13000                # Reserve a port for your service.
    data = {}
    keyy='IROCKKKKKKKK'
    kevVal=hashlib.sha256(keyy).hexdigest()
    data['KEY'] = kevVal
    data['METHOD'] = 'PUT'
    data['VALUE'] = 'THISISAWESOME'
    data['TIMESTAMP']=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
    #data['TIMESTAMP']=datetime.date.today().strftime("%Y-%m-%d %H:%M:%S.%f")
    vector_clock = {}
    vector_clock['A'] = '1'
    vector_clock['B'] = '2'
    vector_clock['C'] = '10'
    vector_clock["Tylers-MBP-2/192.168.1.73"]='3'
    data['VECTOR_CLOCK'] = vector_clock
    json_data = json.dumps(data)
    s.connect((host, port))
    s.send(b''+json_data+'\n')
    print s.recv(1024)
    s.close()                     # Close the socket when done
    

def recvall(sock):
    data = ""
    part = None
    while part != "":
        part = sock.recv(10)
        data += part
    return data

if __name__ =='__main__':
    testPut()