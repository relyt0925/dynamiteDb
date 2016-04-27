#!/usr/bin/python           # This is client.py file
import time
import socket               # Import socket module
import json
import argparse
import datetime
import random


parser = argparse.ArgumentParser(description='Client node instance')


parser.add_argument("method", type=str, default='GET', choices=['PUT', 'GET'], help="PUT or GET")   
parser.add_argument("key", type=str, help="Object key")
parser.add_argument("letter", type=str, help="letter to duplicate")
parser.add_argument("byte", type=int, help="bytes of data")

start_time = datetime.datetime.now()

args = parser.parse_args()

key = args.key
method = args.method
letter = args.letter
byte = args.byte

value = letter * byte
# print 'key:' + key
# print 'value is ' + str(byte) + 'bytes of the letter ' + letter

s = socket.socket()         # Create a socket object
host= 'masterLoadBalancer-759916868.us-east-1.elb.amazonaws.com'
# host = '52.3.240.101'
port = 12415                # Reserve a port for your service.

data = {}
data['METHOD'] = method
data['KEY'] = key
data['VALUE'] = value

j_dump=json.dumps(data)
s.connect((host, port))

reply=(s.recv(1024))
# print reply

s.send(j_dump)

reply=json.loads(s.recv(1024))
# print reply

s.close()                    # Close the socket when done

end_time = datetime.datetime.now()

duration = end_time - start_time

duration = duration.total_seconds()

print "bytes,"+str(byte)+",letter," + letter + ",key," + key + ",duration," + str(duration) 