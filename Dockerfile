FROM node:12

ARG DEBIAN_FRONTEND=noninteractive
RUN apt-get update
RUN apt-get -y install git \
		wget \
		vim \
		openjdk-8-jdk \
		librdkafka-dev \
		python-pip \
		cmake
RUN pip install kafka-python
RUN npm install express-generator -g cors mysql2

WORKDIR /home/workspace

# http, tcp, websocket
EXPOSE 3000 5080 3001

