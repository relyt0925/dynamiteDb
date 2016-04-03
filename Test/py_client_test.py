#!/usr/bin/python           # This is client.py file

import socket               # Import socket module
import json

s = socket.socket()         # Create a socket object
host = 'localhost' # Get local machine name
port = 9898                # Reserve a port for your service.
data = {}
data['KEY'] = 'dummy_key'
data['METHOD'] = 'GET'
data['VALUE'] = 'dummy_val'
vector_clock = {}
vector_clock['1'] = 'A'
vector_clock['2'] = 'B'
vector_clock['3'] = 'C'
data['VECTOR_CLOCK'] = vector_clock
json_data = json.dumps(data)
s.connect((host, port))
s.send(b''+json_data+'\n')

s.close                     # Close the socket when done
