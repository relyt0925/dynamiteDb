#Master-Client-DummydbNode configuration<br/>

-Client sends key-value pair in a dictionary as a json object to master<br/>
-Master unpacks this json, sends it to dummy dbNode<br/>
-dummyDB updates it's dictionary of key-value pairs with this new entry, checks for the new entrys key in it's dictionary (which will always be a hit because we just added it) and sends back the value to master<br/>
-master sends back this value as a json object to the client.<br/>


run on three different terminals in the given order (or run dbnode1.py,dbnode2.py,dbnode3.py and master.py in background):<br/>
python dbnode1.py<br/>
python dbnode2.py<br/>
python dbnode3.py<br/>
python master.py<br/>
python client.py<br/>
python client1.py<br/>

