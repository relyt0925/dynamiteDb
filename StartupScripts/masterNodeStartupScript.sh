#!/bin/sh
wget -O /home/ubuntu/dynamiteDbRepo/dynamiteDb/src/main/resources/ip/hostIp.conf http://169.254.169.254/latest/meta-data/public-ipv4
cd /home/ubuntu/dynamiteDbRepo/Python/Src/
python master.py &
python heartbeatServer.py &
