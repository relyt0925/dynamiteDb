#!/bin/bash
letterlist=(Y)
bytelist=(128)
keylist=(key1 key2 key3 key4 key5 key6 key7 key8 key9 key10 key11 key12 key13 key14 key15 key16 key17 key18 key19 key20)


for byte in ${bytelist[@]};
do
	for letter in ${letterlist[@]}; 
	do 
		for key in ${keylist[@]};   
		do
			echo Putting value $letter of $byte bytes for key: $key
			python test_client.py 'PUT' $key $letter $byte >> puttest$byte.txt
			sleep 3s
		done
		wait ${!}
	done
	sleep 1m
done


for byte in ${bytelist[@]};
do
	for letter in ${letterlist[@]}; 
	do 
		for key in ${keylist[@]};   
		do
			echo Getting value $letter of $byte bytes for key: $key
			python test_client.py 'GET' $key $letter $byte >> gettest$byte.txt
			sleep 3s
		done
		wait ${!}
	done
done


