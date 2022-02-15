#!/bin/bash

sudo docker run --gpus all -it --name kafka_server -itd --net loadcell_bridge --ip 172.19.0.5 -p 3000:3000 -p 5080:5080 -p 3001:3001 -v $PWD:/home/workspace loadcell:1.0 /bin/bash
