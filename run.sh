#!/bin/bash
/usr/sbin/pdns_server --daemon=no --allow-recursion=172.17.0.0/24 --disable-axfr=yes --local-address=0.0.0.0 --launch=pipe --pipe-command='/data/pipe-consul --environment=balance --address=consul1 -logtostderr=true' --pipebackend-abi-version=4
