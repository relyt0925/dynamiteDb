#!/bin/bash
letterlist=(a b c d e f g h i j)
byte=256
keylist=(key7)

for letter in ${letterlist[@]}; 
do 
	for key in ${keylist[@]};   
	do
		echo Putting value $letter of $byte bytes for key: $key
		python test_client.py 'PUT' $key $letter $byte >> putnumlogs.txt & 
	done
	:
	wait ${!}
	for key in ${keylist[@]};  
	do
		echo Reading value of key:$key
		python test_readDB.py 'GET' $key $letter $byte >> onediffnumkeyslog.txt &
	done
	:
	wait ${!}
done
echo done with one key
sleep 5s

keylist=(key7 key13 key14 key18 key32 key36 key39 key41 key43 key44)

echo starting ten keys
for letter in ${letterlist[@]}; 
do 
	for key in ${keylist[@]};   
	do
		echo Putting value $letter of $byte bytes for key: $key
		python test_client.py 'PUT' $key $letter $byte >> putnumlogs.txt & 
	done
	:
	wait ${!}
	for key in ${keylist[@]};  
	do
		echo Reading value of key:$key
		python test_readDB.py 'GET' $key $letter $byte >> tendiffnumkeyslog.txt &
	done
	:
	wait ${!}
done
echo done with ten keys
sleep 10s

keylist=(key7 key13 key14 key18 key32 key36 key39 key41 key43 key44 key49 key50 key51 key52 key57 key72 key80 key83 key90 key92 key100 key102 key103 key105 key110 key119 key120 key122 key128 key131 key132 key133 key135 key140 key141 key148 key152 key153 key155 key156 key157 key160 key164 key169 key170 key172 key174 key180 key181 key187)

echo starting fifty keys
for letter in ${letterlist[@]}; 
do 
	for key in ${keylist[@]};   
	do
		echo Putting value $letter of $byte bytes for key: $key
		python test_client.py 'PUT' $key $letter $byte >> putnumlogs.txt & 
	done
	:
	wait ${!}	
	for key in ${keylist[@]};  
	do
		echo Reading value of key:$key
		python test_readDB.py 'GET' $key $letter $byte >> fiftydiffnumkeyslog.txt &
	done
	:
	wait ${!}
echo done with fifty keys
done
