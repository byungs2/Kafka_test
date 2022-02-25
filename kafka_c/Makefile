CC=gcc
CFLAGS=`pkg-config --cflags rdkafka gstreamer-1.0`
LIBS=`pkg-config --libs rdkafka gstreamer-1.0`

TARGET=producer

SRC=producer.c
$(TARGET): $(SRC)
	$(CC) $(CFLAGS) -o $(TARGET) $(SRC) $(LIBS)
