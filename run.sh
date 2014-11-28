#!/bin/bash
/usr/sbin/pdns_server --daemon=no --allow-recursion=172.17.0.0/24 --disable-axfr=yes --local-address=0.0.0.0 --launch=pipe --pipe-command='/data/pipe-consul -logtostderr=true --auth-domain=.env.plus.net --consul-conn=http://consul' --pipebackend-abi-version=4
