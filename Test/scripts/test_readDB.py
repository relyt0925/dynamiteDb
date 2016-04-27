#!/usr/bin/python           # This is client.py file
import time
import socket               # Import socket module
import json
import argparse
import datetime
import random
import hashlib

start_time = datetime.datetime.now()


parser = argparse.ArgumentParser(description='Client node instance')

parser.add_argument("method", type=str, default='GET', choices=['PUT', 'GET'], help="PUT or GET")   
parser.add_argument("key", type=str, help="Object key")
parser.add_argument("letter", type=str, help="letter to duplicate")
parser.add_argument("byte", type=int, help="bytes of data")

args = parser.parse_args()

key = args.key
method = args.method
letter = args.letter
byte = args.byte

updated_reply = letter * byte

key_hash=hashlib.sha256(key).hexdigest()

data = {}
data['METHOD'] = method
data['KEY'] = key_hash

updated = 0
# duration = 5
while updated == 0:
    s = socket.socket()         # Create a socket object
    # s.bind(('24.30.41.163',3899))
    host= '52.200.254.246' #dbnode4's IP address
    port = 13000                # Reserve a port for your service.

    j_dump=json.dumps(data)
    s.connect((host, port))
    # print j_dump
    s.send(b''+j_dump+'\n')

    reply=json.loads(s.recv(1024))
    # print reply
    s.close()                    # Close the socket when done
    if 'VALUE' in reply:
        # print reply['VALUE']
        if reply['VALUE'] == updated_reply:
            updated = 1
            end_time = datetime.datetime.now()
            duration = end_time - start_time
            duration = duration.total_seconds()

    time.sleep(0.5)

print "bytes,"+str(byte)+",letter," + letter + ",key," + key + ",duration," + str(duration) 

