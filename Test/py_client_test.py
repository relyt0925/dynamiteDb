#!/usr/bin/python           # This is client.py file

import socket               # Import socket module
import json
import hashlib
import datetime

def testGet(host):
    s = socket.socket()         # Create a socket object
    #host = '52.201.0.131' # Get local machine name
    port = 13000                # Reserve a port for your service.
    data = {}
    keyy='IRKKKKKLAAAAAAAAA'
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
    
def testPut(host):
    s = socket.socket()         # Create a socket object
    #host = '52.201.0.131' # Get local machine name
    port = 13000                # Reserve a port for your service.
    data = {}
    keyy='IRKKKKKLAAAAAAAAA'
    kevVal=hashlib.sha256(keyy).hexdigest()
    data['KEY'] = kevVal
    data['METHOD'] = 'PUT'
    data['VALUE'] = 'THIFEW'
    data['TIMESTAMP']=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
    #data['TIMESTAMP']=datetime.date.today().strftime("%Y-%m-%d %H:%M:%S.%f")
    vector_clock = {}
    vector_clock['A'] = '1'
    vector_clock['B'] = '2'
    vector_clock['C'] = '11'
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
    #testPut('localhost')
    testGet('52.200.241.247')
    testGet('52.200.254.246')
    testGet('52.200.248.223')
    testGet('52.200.255.102')
    testGet('52.201.0.131')