#!/usr/bin/python           # This is client.py file
import time
import socket               # Import socket module
import json
import argparse
import datetime
import random

cmd = 0 #toggle to use cmd or static key/value

method = 'PUT'
# key='dummy_key'
key = str(random.randint(0,100))
print key
value = 'Yaksh'
timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")



if cmd == 1:
	parser = argparse.ArgumentParser(description='Client node instance')

	parser.add_argument("method", type=str, default='GET', choices=['PUT', 'GET'], help="PUT or GET")	
	parser.add_argument("key", type=str, help="Object key")
	parser.add_argument("value", type=str, help="Object value")

	args = parser.parse_args()

	key = args.key
	value = args.value
	method = args.method

s = socket.socket()         # Create a socket object
host= 'masterLoadBalancer-759916868.us-east-1.elb.amazonaws.com'
#host = '54.85.66.252' # Get local machine name
port = 12415                # Reserve a port for your service.


data = {}
data['METHOD'] = method
data['KEY'] = key
data['VALUE'] = value
# data['TIMESTAMP'] = timestamp

j_dump=json.dumps(data)
s.connect((host, port))

reply=(s.recv(1024))
print reply

s.send(j_dump)

reply=json.loads(s.recv(1024))
print reply

s.close()                    # Close the socket when done
