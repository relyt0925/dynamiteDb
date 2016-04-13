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
testip = '54.85.66.252'
hashtestip = hashlib.sha256(testip).hexdigest()
dbnodes_ip_hash=[]
dbnodes_ip=[]

masterid = 'Master1/54.85.66.252'

HOST = testip    # Symbolic name meaning all available interfaces
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
            # print i
            # print ip_array[0]
            ip_array[1] = dbnodes_ip[(i+1) % 5]
            # print ip_array[1]
            # print i+2
            # print (i+2) % 5
            ip_array[2] = dbnodes_ip[(i+2) % 5]
            # print ip_array[2]
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
    
    with open('systemips.conf') as fp:
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
            # ip='192.168.56.1'
            # print hashlib.sha256(ip).hexdigest()
            data['KEY'] = key_hash

            if(data['METHOD']== 'GET'): 
                rep=get_from_dbnodes(data,0)
                if(rep=='404'):
                    rep='Key Not Found'
            elif(data['METHOD']=='PUT'):
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

def get_from_dbnode3(data,ip):
    s3 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)         # Create a socket object
    # bind_port= 12334
    print 'trying to get from 3 node = ' +str(ip)
    try:
        s3.bind(('54.85.66.252', 0))
        print s3.getsockname()
    except socket.error as msg:
        print 'Error Code : ' + str(msg)
        sys.exit()
    host1 = ip      # Get local machine name
    port1 = 13000                # Reserve a port for your service.
    
    s3.settimeout(1)

    try:
        s3.connect((host1, port1))

        j_dump=json.dumps(data)
        s3.send(b''+j_dump+'\n')
    except socket.error as msg:
        print 'Error Code : ' + str(msg)
        return '404'
    #set timeout on receive to avoid deadlock
    #if reply='404' then key not found, otherwise reply='OK'
    print 'blocking get node 3=' + str(ip)

    

    try:
        # print 'here\n'
        reply3=json.loads(s3.recv(1024))
        print reply3
    except socket.error as err:
        reply3='404'
        print (err)    

    s3.close()
    return reply3



def post_to_dbnode3(data,ip):
    s3 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)         # Create a socket object
    # bind_port= 12334
    try:
        s3.bind(('54.85.66.252', 0))
        print s3.getsockname()
    except socket.error as msg:
        print 'Error Code : ' + str(msg)
        sys.exit()
    host1 = ip      # Get local machine name
    port1 = 13000                # Reserve a port for your service.
    
    s3.settimeout(1)

    try:
        s3.connect((host1, port1))

        j_dump=json.dumps(data)
        s3.send(b''+j_dump+'\n')
    except socket.error as msg:
        print 'Error Code : ' + str(msg)
        return '404'
    #set timeout on receive to avoid deadlock
    #if reply='404' then key not found, otherwise reply='OK'
    print 'blocking post node 3=' + str(ip)
    

    try:
        # print 'here\n'
        reply3=json.loads(s3.recv(1024))
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

    print 'in post acquire'

    server_fail=0

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
       data['VECTOR_CLOCK'] = {masterid:1}
       data['TIMESTAMP'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
    else:
        data['VECTOR_CLOCK'] = getdata['VECTOR_CLOCK']
        data['VECTOR_CLOCK'][masterid] = data['VECTOR_CLOCK'].get(masterid, 0) + 1  #increment its own VC by 1 or add entry of value 1
        data['TIMESTAMP'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")

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
    
    host1 = ip_array[0].split("\n")[0]      # Get local machine name
    port1 = 13000
    host2 = ip_array[1].split("\n")[0]      # Get local machine name
    port2 = 13000

    j_dump=json.dumps(data)

    try:
        s1.bind(('54.85.66.252', 0))
        s2.bind(('54.85.66.252', 0))
        print s1.getsockname()
        print s2.getsockname()
    except socket.error as msg:
        print 'Error Code : ' + str(msg)
        mutex.release()
        sys.exit()

    print ip_array[0].split("\n")[0]
    s1.settimeout(1)
    s2.settimeout(1)

    try: 
        s1.connect((host1, port1))
        s1.send(b''+j_dump+'\n')
        reply1=json.loads(s1.recv(1024))
        print reply1
    except socket.error as msg:
        print 'Error Code : ' + str(msg) + str(host1)
        recent_reply="Server Down,Please Try again in a few seconds."
        # s1.close()
        # s2.close()
        server_fail=server_fail+1

        reply1='404'
        if(server_fail==2):

            mutex.release()        
            s1.close()
            s2.close()     
            # mutex.release()
            return recent_reply        

    try:
        s2.connect((host2, port2))
        s2.send(b''+j_dump+'\n')
        reply2=json.loads(s2.recv(1024))
        print reply2
    except socket.error as msg:
        print 'Error Code : ' + str(msg) + str(host2)
        recent_reply="Server Down,Please Try again in a few seconds."
        # s1.close()
        # s2.close()
        server_fail=server_fail+1

        reply2='404'
        if(server_fail==2):

            mutex.release()        
            s1.close()
            s2.close()     
            # mutex.release()
            return recent_reply 


    if(reply1!='404' and reply2!='404'):
        s1.close()
        s2.close()
        mutex.release()
        return 'POST SUCCESS'

    elif((reply1=='404' and reply2!='404') or (reply1!='404' and reply2=='404') ):
        # Since one of the two primary servers failed 
        # to respond try the third server.
        n3_rep=post_to_dbnode3(data,ip_array[2].split("\n")[0])
        if (n3_rep!='404'):
            s1.close()
            s2.close()
            mutex.release()
            return 'POST SUCCESS'
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
 
    print 'in get'

    server_fail=0

    if(flag==0):   
        mutex.acquire()

    s1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)         # Create a socket object
    s2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)          # Create a socket object
    s1.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s2.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)    
    # bind_port= 12336
    # bind_port1=12337
    s1.settimeout(1)
    s2.settimeout(1)

    try:
        s1.bind(('54.85.66.252', 0))
        s2.bind(('54.85.66.252', 0))
        print s1.getsockname()
        print s2.getsockname()
    except socket.error as msg:
        print 'Error Code : ' + str(msg)
        if(flag==0):
            mutex.release()
        sys.exit()

    ip_array=lookup_ip(data['KEY'])

    host1 = ip_array[0].split("\n")[0]       # Get local machine name
    port1 = 13000                # Reserve a port for your service.
    

    host2 = ip_array[1].split("\n")[0]       # Get local machine name
    port2 = 13000                # Reserve a port for your service.

    print 'trying to connect to ' + host1 + ':' + str(port1) +' and ' + host2 + ':' + str(port2)

    print data
    j_dump=json.dumps(data)

    try: 
        s1.connect((host1, port1))
        s1.send(b''+j_dump+'\n')
        reply1=json.loads(s1.recv(1024))
        print reply1
    except socket.error as msg:
        print 'Error Code : ' + str(msg) + str(host1)
        recent_reply="Server Down,Please Try again in a few seconds."
        # s1.close()
        # s2.close()
        server_fail=server_fail+1
        reply1='404'
        if(server_fail==2):
            if(flag==0):
                mutex.release()        
            s1.close()
            s2.close()     
            # mutex.release()
            return recent_reply        

    try:
        s2.connect((host2, port2))
        s2.send(b''+j_dump+'\n')
        reply2=json.loads(s2.recv(1024))
        print reply2
    except socket.error as msg:
        print 'Error Code : ' + str(msg) + str(host2)
        recent_reply="Server Down,Please Try again in a few seconds."
        # s1.close()
        # s2.close()
        server_fail=server_fail+1
        reply2='404'
        if(server_fail==2):
            if(flag==0):
                mutex.release()        
            s1.close()
            s2.close()     
            # mutex.release()
            return recent_reply 


    if(server_fail==1):
        if(reply1=='404'):
            reply1=get_from_dbnode3(data,ip_array[2].split("\n")[0])
            if(reply1=='404'):
                if(flag==0):
                    mutex.release()
                return "Server Down,Please Try again in a few seconds."
        else:
            reply2=get_from_dbnode3(data,ip_array[2].split("\n")[0])
            if(reply2=='404'):
                if(flag==0):
                    mutex.release()
                return "Server Down,Please Try again in a few seconds."

    print reply1
    print reply2

    if(reply1['METHOD']=='NOVAL' or reply2['METHOD']=='NOVAL'):
        if(flag==0):
            mutex.release()
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
    print socket.gethostbyname(socket.gethostname())
    print 'Connected with ' + addr[0] + ':' + str(addr[1])
     
    #start new thread takes 1st argument as a function name to be run, second is the tuple of arguments to the function.
    start_new_thread(clientthread ,(conn,))
 
s.close()  