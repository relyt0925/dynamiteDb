#Basic Master-Client-DummydbNode configuration

-Client sends key-value pair in a dictionary as a json object to master
-Master unpacks this json, sends it to dummy dbNode
-dummyDB updates it's dictionary of key-value pairs with this new entry, checks for the new entrys key in it's dictionary (which will always be a hit because we just added it) and sends back the value to master
-master sends back this value as a json object to the client.


run on three different terminals (or run dbnode1.py and master.py in background):
python master.py
python client.py
python dbnode1.py
