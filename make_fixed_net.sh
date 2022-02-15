#!/bin/bash

sudo docker network create --gateway 172.19.0.1 --subnet 172.19.0.0/21 loadcell_bridge 
