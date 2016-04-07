import socket
import sys
from thread import *
import datetime
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

mutex = Lock()

dbnodes_ip_hash=[]
dbnodes_ip=[]
rcount = 2
wcount = 2
replicas = 3

HOST = ''    # Symbolic name meaning all available interfaces
PORT = 12415 # Arbitrary non-privileged port

class KeyValueInfo:
    def __init__(self, key, value, time_stamp):
        self.key = key
        self.value = value
        self.time_stamp = time_stamp


def lookup_ip(key_hash):

    print'looking up ip'

    global dbnodes_ip,dbnodes_ip_hash
    prev = 0
    ip_array= [None] * 3
    for i,ip_hash in enumerate(dbnodes_ip_hash):

        if(key_hash > prev and key_hash <= ip_hash):
            ip_array[0] = dbnodes_ip[i]
            ip_array[1] = dbnodes_ip[i+1 % 5]
            ip_array[2] = dbnodes_ip[i+2 % 5]
            print 'found in loop'
            return ip_array

        else:
            prev = ip_hash

    ip_array[0] = dbnodes_ip[0]
    ip_array[1] = dbnodes_ip[1]
    ip_array[2] = dbnodes_ip[2]
    print 'not found in loop'
    return ip_array




def load_ip_hash():

    global dbnodes_ip,dbnodes_ip_hash
    
    with open('test_config.txt') as fp:
        for line in fp:
            if line:
                dbnodes_ip_hash.append(line.split(",")[0])
                dbnodes_ip.append(line.split(",")[1])
                
    print dbnodes_ip


#Function for handling connections. This will be used to create threads
def clientthread(conn):
    #Sending message to connected client
    conn.send('Welcome to the master server. \n') #send only takes string
     
    #infinite loop so that function do not terminate and thread do not end.
    while True:
         
        #Receiving from client
        try:
            rep="Server Down,Please Try again in a few seconds"
            data = json.loads(conn.recv(1024))

            # hashkey = hashlib.sha256(data['KEY']).hexdigest()   #key hashing. uncomment this and the next line.
            # data['KEY'] = hashkey
            
            
            client_key=data['KEY']
            key_hash=hashlib.sha256(client_key).hexdigest()
            # ip='24.72.242.230'
            # print hashlib.sha256(ip).hexdigest()
            data['KEY'] = key_hash

            if(data['METHOD']== 'GET'): 
                rep=get_from_dbnodes(data,0)
                if(rep=='404'):
                    rep='Key Not Found'
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


def post_to_dbnode3(data,ip):
    s3 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)         # Create a socket object
    # bind_port= 12334
    try:
        s3.bind(('localhost', 0))
        print s3.getsockname()
    except socket.error as msg:
        print 'Bind failed. Error Code : ' + str(msg[0]) + ' Message ' + msg[1]
        sys.exit()
    host1 = ip      # Get local machine name
    port1 = 12357                # Reserve a port for your service.
    
    try:
        s3.connect((host1, port1))

        j_dump=json.dumps(data)
        s3.send(j_dump)
    except socket.error as msg:
        print 'Bind failed. Error Code : ' + str(msg[0]) + ' Message ' + msg[1]
        return '404'
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
    # acquire mutex incase of multiple connections to same
    # db servers. Otherwise,If one connection completes, the port will 
    # be closed and will result in unpredictable behavior for the other connection.
    # Also, the sends from the dbnodes will be coming to the same port and IP and
    # two multiple connections won't be able to differentiate between the two.
    print 'in post acquire'

    mutex.acquire()

    getdata = data.copy()
    getdata['METHOD'] = 'GET'
    getdata = get_from_dbnodes(getdata,1)
    print 'I finished get'
    print getdata

    if(getdata=='Server Down,Please Try again in a few seconds.'):
        mutex.release()
        return 'Server Down,Please Try again in a few seconds.'
    elif(getdata=='404'):
       data['VECTOR_CLOCK'] = {}
       data['TIMESTAMP'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
    else:
        data['VECTOR_CLOCK'] = getdata['VECTOR_CLOCK']
        data['TIMESTAMP'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
    # needs to increase its own vector clock by 1

    ip_array=lookup_ip(data['KEY'])
    print ip_array
   
    s1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)        # Create a socket object
    s2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)         # Create a socket object

    print s1
    print s2

    s1.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s2.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    # bind_port= 12333
    # bind_port1=12335
    
    try:
        s1.bind(('', 0))
        s2.bind(('', 0))
        print s1.getsockname()
        print s2.getsockname()
    except socket.error as msg:
        print 'Bind failed. Error Code : ' + str(msg[0]) + ' Message ' + msg[1]
        mutex.release()
        sys.exit()
    print ip_array[0].split("\n")[0]
    host1 = ip_array[0].split("\n")[0]      # Get local machine name
    port1 = 12338                # Reserve a port for your service.
    s1.connect((host1, port1))

    host2 = ip_array[1].split("\n")[0]      # Get local machine name
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
        mutex.release()
        return 'POST SUCCESS'

    elif((reply1=='404' and reply2=='OK') or (reply1=='OK' and reply2=='404') ):
        # Since one of the two primary servers failed 
        # to respond try the third server.
        n3_rep=post_to_dbnode3(data,ip_array[2].split("\n")[0])
        if (n3_rep=='OK'):
            s1.close()
            s2.close()
            mutex.release()
            return 'POST SUCESS'
        else:
	        s1.close()
	        s2.close()
	        mutex.release()
	        return 'Error: 2 of 3 Servers Down, Please try again in a few seconds'
    else:
        mutex.release()
        return 'Error: 2 of 3 Servers Down, Please try again in a few seconds (Server 1 and Server 2)'


# def ip_lookup(hash_key):
def get_from_dbnodes(data,flag):
    # acquire mutex incase of multiple connections to same
    # db servers. Otherwise,If one connection completes, the port will 
    # be closed and will result in unpredictable behavior for the other connection.
    # Also, the sends from the dbnodes will be coming to the same port and IP and
    # two multiple connections won't be able to differentiate between the two. 
    print 'in get'

    if(flag==0):   
        mutex.acquire()

    s1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)         # Create a socket object
    s2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)          # Create a socket object
    s1.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s2.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)    
    # bind_port= 12336
    # bind_port1=12337
    s1.settimeout(2)
    s2.settimeout(2)

    try:
        s1.bind(('localhost', 0))
        s2.bind(('localhost', 0))
        print s1.getsockname()
        print s2.getsockname()
    except socket.error as msg:
        print 'Bind failed. Error Code : ' + str(msg[0]) + ' Message ' + msg[1]
        if(flag==0):
            mutex.release()
        sys.exit()

    ip_array=lookup_ip(data['KEY'])

    host1 = ip_array[0].split("\n")[0]       # Get local machine name
    port1 = 12338                # Reserve a port for your service.
    

    host2 = ip_array[1].split("\n")[0]       # Get local machine name
    port2 = 12359                # Reserve a port for your service.

    try:
        print 'trying to connect'
        s1.connect((host1, port1))
        s2.connect((host2, port2))
    except socket.error as msg:
        print 'Error Code : ' + str(msg[0]) + ' Message ' + msg[1]
        recent_reply="Server Down,Please Try again in a few seconds."
        s1.close()
        s2.close()
        if(flag==0):
            mutex.release()
        return recent_reply

    print data
    j_dump=json.dumps(data)

    try:
        s1.send(j_dump)
        s2.send(j_dump)
        reply1=json.loads(s1.recv(1024))    
        reply2=json.loads(s2.recv(1024))
        print reply1
        print reply2
    except socket.error as msg:
        print 'Error Code : ' + str(msg[0]) + ' Message ' + msg[1]
        recent_reply="Server Down,Please Try again in a few seconds."
        s1.close()
        s2.close()
        if(flag==0):
            mutex.release()
        return recent_reply

    if(reply1=='404' or reply2=='404'):
        return '404'

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

    print 'recent reply:' + str(recent_reply)
    if(flag==0):
        mutex.release()

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

load_ip_hash()

#now keep talking with the client
while 1:
    #wait to accept a connection - blocking call
    conn, addr = s.accept()
    # me=socket.gethostbyname(socket.gethostname())
    # server_ip_digest=hashlib.sha256(me).digest()
    # dbnodes_ip_hash[me]=server_ip_digest
    # print dbnodes_ip_hash
    print 'Connected with ' + addr[0] + ':' + str(addr[1])
     
    #start new thread takes 1st argument as a function name to be run, second is the tuple of arguments to the function.
    start_new_thread(clientthread ,(conn,))
 
s.close()  