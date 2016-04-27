#!/bin/bash
letterlist=(a b c d e f g h i j)
#letterlist=z
bytelist=(512)
#bytelist=1
keylist=(key7 key13 key14 key18 key32 key36 key39 key41 key43 key44)
for byte in ${bytelist[@]};
do
	for letter in ${letterlist[@]}; 
	do 
		for key in ${keylist[@]};   
		do
			echo Putting value $letter of $byte bytes for key: $key
			python test_client.py 'PUT' $key $letter $byte >> putvallogs$byte.txt & 
		done
		:
		wait ${!}
		
		for key in ${keylist[@]};  
		do
			echo Reading value of key:$key
			python test_readDB.py 'GET' $key $letter $byte >> diffvallengthlog$byte.txt &
		done
		:
		wait ${!}
		sleep 10s
	done
done
