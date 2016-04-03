#!/usr/bin/python           # This is client.py file
import time
import socket               # Import socket module
import json
import argparse

# METHOD = ''

# parser = argparse.ArgumentParser(description='Client Node')
# parser.add_argument('-m', '-method', '-METHOD', help='METHOD TYPE', required=True, default = 'PUT', choices = ['GET', 'PUT'])
# args = vars(parser.parse_args())

s = socket.socket()         # Create a socket object
host = socket.gethostname() # Get local machine name
port = 12345                # Reserve a port for your service.

# data={"yakshdeepk":("value = 51","Feb 12 08:02:32 2013")}
data = {}
data['KEY'] = 'dummy_key'
data['METHOD'] = 'GET'
data['VALUE'] = 'dummy_val'

j_dump=json.dumps(data)
s.connect((host, port))

reply=(s.recv(1024))
print reply

s.send(j_dump)
time.sleep(1)
time.sleep(1)
reply=json.loads(s.recv(1024))
print reply

s.close()                    # Close the socket when done