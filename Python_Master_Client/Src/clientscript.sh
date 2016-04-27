#!/bin/bash
for i in {1..50}; do
    for j in {1..50}; do
     k=$i*$j
     echo -e "\nROUND $k\n"

    python client.py &
    python client1.py &
  done
  wait
done 2>/dev/null
