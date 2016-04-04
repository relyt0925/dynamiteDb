#!/usr/bin/python           # This is client.py file
import time
import socket               # Import socket module
import json
import argparse

s = socket.socket()         # Create a socket object
host = socket.gethostname() # Get local machine name
port = 12365                # Reserve a port for your service.


data = {}
data['KEY'] = 'dummy_key'
data['METHOD'] = 'POST'
data['VALUE'] = 'hello_upd12'

j_dump=json.dumps(data)
s.connect((host, port))

reply=(s.recv(1024))
print reply

s.send(j_dump)

reply=json.loads(s.recv(1024))
print reply

s.close()                    # Close the socket when done