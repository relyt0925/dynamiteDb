import socket
import sys
from thread import *
from datetime import datetime
import json
import errno
import hashlib
import urllib
import urllib2
import time

value=["yaksh's key","yaksh's value_new","yaksh's key","yaksh's value_old"]
timestmp=["yaksh's key","Feb 12 08:02:32 2013","yaksh's key","Jan 27 11:52:02 2011"]
dbnodes_ip_hash={}
mydict={}

HOST = ''   # Symbolic name meaning all available interfaces
PORT = 12345 # Arbitrary non-privileged port

class KeyValueInfo:
    def __init__(self, key, value, time_stamp):
        self.key = key
        self.value = value
        self.time_stamp = time_stamp


#Function for handling connections. This will be used to create threads
def clientthread(conn):
    #Sending message to connected client
    conn.send('Welcome to the server. \n') #send only takes string
     
    #infinite loop so that function do not terminate and thread do not end.
    while True:
         
        #Receiving from client
        try:
            rep="key not found"
            data = json.loads(conn.recv(1024))
            # j_load= json.loads(data)
            mydict.update(data)
            rep=ask_dbnodes(data)
            # print data[data.keys()[0]][0]
            # print mydict
        except ValueError:
            print "Nothing Received \n"

        # count=0
        # rep="key not found"
        # time="Jan 1 00:00:00 1999"
        # time=datetime.strptime(time, "%b %d %H:%M:%S %Y")
        # for i in value:
        #   count=count+1
        #   if(i==data):
        #         if(max((time,datetime.strptime(timestmp[count], "%b %d %H:%M:%S %Y")))==datetime.strptime(timestmp[count], "%b %d %H:%M:%S %Y")):
        #           time=datetime.strptime(timestmp[count], "%b %d %H:%M:%S %Y")
        #           print time
        #           rep=value[count]
        #         print rep
                
        try:
            conn.send(rep)
        except IOError, e:
            if e.errno == errno.EPIPE:
                print "Client Closed Connection \n"
                break
        # reply = 'OK...' + data
        if not data: 
            break
     
        # conn.sendall(reply)
     
    #came out of loop
    conn.close()
 

# def ip_lookup(hash_key):
def ask_dbnodes(data):
    s1 = socket.socket()         # Create a socket object
    host1 = '24.72.242.230' # Get local machine name
    port1 = 12348             # Reserve a port for your service.
    s1.connect((host1, port1))
    j_dump=json.dumps(data)
    s1.send(j_dump)
    time.sleep(1)
    reply=s1.recv(1024)
    print reply
    s1.close()
    return reply


s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
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

#now keep talking with the client
while 1:
    #wait to accept a connection - blocking call
    conn, addr = s.accept()
    me=socket.gethostbyname(socket.gethostname())
    server_ip_digest=hashlib.sha256(me).digest()
    dbnodes_ip_hash[me]=server_ip_digest
    print dbnodes_ip_hash
    print 'Connected with ' + addr[0] + ':' + str(addr[1])
     
    #start new thread takes 1st argument as a function name to be run, second is the tuple of arguments to the function.
    start_new_thread(clientthread ,(conn,))
 
s.close()