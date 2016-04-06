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
import select
from collections import Counter 
from itertools import chain
from threading import Thread, Lock

#unmutex mutex = Lock()

dbnodes_ip_hash={}
rcount = 2
wcount = 2
replicas = 3

HOST = ''    # Symbolic name meaning all available interfaces
PORT = 12365 # Arbitrary non-privileged port

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

            # hashkey = hashlib.sha256(data['KEY']).hexdigest()   #key hashing. uncomment this and the next line.
            # data['KEY'] = hashkey

            if(data['METHOD']== 'GET'): 
                rep=get_from_dbnodes(data)
            elif(data['METHOD']=='POST'):
                rep=post_to_dbnodes(data)
            else:
                rep='Error: Invalid Method Parameter'
            # print data[data.keys()[0]][0]
            # print mydict
        except ValueError:
            print "Nothing Received \n"
        except IOError, e:
            if e.errno == errno.EPIPE:
                print "Client Closed Connection \n"
                break
                
        try:
            conn.send(json.dumps(rep))
        except IOError, e:
            if e.errno == errno.EPIPE:
                print "Client Closed Connection \n"
                break
        # reply = 'OK...' + data
        if not data: 
            break
     
     
    #came out of loop
    conn.close()


def post_to_dbnode3(data):
    s3 = socket.socket()         # Create a socket object

    host1 = '192.168.56.1'     # Get local machine name
    port1 = 12357               # Reserve a port for your service.
    s3.connect((host1, port1))

    j_dump=json.dumps(data)
    s3.send(j_dump)

    #set timeout on receive to avoid deadlock
    #if reply='404' then key not found, otherwise reply='OK'
    print 'blocking node 3'
    s3.settimeout(2)

    try:
        # print 'here\n'
        reply3=s3.recv(1024)
        print reply3
    except socket.error as err:
        reply3='404'
        print (err)    

    s3.close()
    return reply3
 
def post_to_dbnodes(data):
    # acquire #unmutex mutex incase of multiple connections to same
    # db servers. Otherwise,If one connection completes, the port will 
    # be closed and will result in unpredictable behavior for the other connection.
    # Also, the sends from the dbnodes will be coming to the same port and IP and
    # two multiple connections won't be able to differentiate between the two.
    #unmutex mutex.acquire()
    getdata = data.copy()
    getdata['METHOD'] = 'GET'
    getdata = get_from_dbnodes(getdata)
    print 'I finished get'
    data['VECTOR_CLOCK'] = getdata['VECTOR_CLOCK']
    # needs to increase its own vector clock by 1
   
    s1 = socket.socket()         # Create a socket object
    s2 = socket.socket()         # Create a socket object

    host1 = '192.168.56.1'      # Get local machine name
    port1 = 12358                # Reserve a port for your service.
    s1.connect((host1, port1))

    host2 = '192.168.56.1'      # Get local machine name
    port2 = 12359                # Reserve a port for your service.
    s2.connect((host2, port2))

    j_dump=json.dumps(data)
    s1.send(j_dump)
    s2.send(j_dump)

    print 'blocking on node 1 and 2'

    #set timeout on receive to avoid deadlock
    #if reply='404' then key not found, otherwise reply='OK'
    s1.settimeout(2)
    s2.settimeout(2)

    try:
        reply2=s2.recv(1024)
        print 'this ' + reply2
    except socket.error as err:
        reply2='404'
        print (err)  

    try:
        reply1=s1.recv(1024)
        print 'this ' + reply1
    except socket.error as err:
        print (err)
        reply1='404'
      

    if(reply1=='OK' and reply2=='OK'):
        s1.close()
        s2.close()
        #unmutex mutex.release()
        return 'POST SUCCESS'

    elif((reply1=='404' and reply2=='OK') or (reply1=='OK' and reply2=='404') ):
        # Since one of the two primary servers failed 
        # to respond try the third server.
        n3_rep=post_to_dbnode3(data)
        if (n3_rep=='OK'):
            s1.close()
            s2.close()
            #unmutex mutex.release()
            return 'POST SUCESS'
        else:
	        s1.close()
	        s2.close()
	        #unmutex mutex.release()
	        return 'Error: 2 of 3 Servers Down, Please try again in a few seconds'
    else:
        #unmutex mutex.release()
        return 'Error: 2 of 3 Servers Down, Please try again in a few seconds (Server 1 and Server 2)'


# def ip_lookup(hash_key):
def get_from_dbnodes(data):
    # acquire #unmutex mutex incase of multiple connections to same
    # db servers. Otherwise,If one connection completes, the port will 
    # be closed and will result in unpredictable behavior for the other connection.
    # Also, the sends from the dbnodes will be coming to the same port and IP and
    # two multiple connections won't be able to differentiate between the two.    
    #unmutex mutex.acquire()
    s1 = socket.socket()         # Create a socket object
    s2 = socket.socket()         # Create a socket object

    host1 = '192.168.56.1'      # Get local machine name
    port1 = 12358                # Reserve a port for your service.
    s1.connect((host1, port1))

    host2 = '192.168.56.1'      # Get local machine name
    port2 = 12359                # Reserve a port for your service.
    s2.connect((host2, port2))

    print data
    j_dump=json.dumps(data)
    s1.send(j_dump)
    s2.send(j_dump)
    reply1=json.loads(s1.recv(1024))    
    reply2=json.loads(s2.recv(1024))
    print reply1
    print reply2

    conflict = conflict_check(reply1['VECTOR_CLOCK'], reply2['VECTOR_CLOCK'])
    merged_vc = dict(sorted(chain(reply1['VECTOR_CLOCK'].items(), reply2['VECTOR_CLOCK'].items()), key=lambda t: t[1]))
    if conflict == 0: #Conflict occured
        #Pick higher timestamp reply and insert merged vector clock 
        print 'conflict occured'
        if reply1['TIMESTAMP'] > reply2['TIMESTAMP'] :
            recent_reply = reply1
        else:
            recent_reply = reply2

    else: #No conflict occured
        if conflict == 1:
            recent_reply = reply1
        elif conflict == 2:
            recent_reply = reply2
    
    recent_reply['VECTOR_CLOCK'] = merged_vc #insert merged vector clock

    s1.close()
    s2.close()

    print recent_reply

    #unmutex mutex.release()

    return recent_reply


def conflict_check(rep1, rep2):
    #Returns 0 if conflict
    #Returns 1 if reply1 is more recent
    #Returns 2 if reply2 is more recent
    print rep1
    print rep2
    rep1 = Counter(rep1)
    rep2 = Counter(rep2)
    rep1.subtract(rep2)
    print rep1

    if (all(rep1[key] >= 0 for key in rep1)): #differences all positive -> rep1 is more recent
        conf = 1
    elif (all(rep1[key] <= 0 for key in rep1)): #differences all negative -> rep2 is more recent
        conf = 2
    else: ##differences are mixed -> conflict in vector clock
        conf = 0

    return conf


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