#!/usr/bin/python           # This is client.py file
import time
import socket               # Import socket module
import json
import argparse
import datetime

cmd = 0 #toggle to use cmd or static key/value

method = 'POST'
key = 'dummy_key'
value = 'hello'
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
host = socket.gethostname() # Get local machine name
port = 12365                # Reserve a port for your service.


data = {}
data['METHOD'] = method
data['KEY'] = key
data['VALUE'] = value
data['TIMESTAMP'] = timestamp

j_dump=json.dumps(data)
s.connect((host, port))

reply=(s.recv(1024))
print reply

s.send(j_dump)

reply=json.loads(s.recv(1024))
print reply

s.close()                    # Close the socket when done