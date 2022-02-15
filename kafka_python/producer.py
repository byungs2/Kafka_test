from kafka import KafkaProducer
import time

def on_send_error(excp):
        log.error('I am an errback', exc_info=excp)

def on_send_success(record_metadata):
    print(record_metadata.offset);

producer = KafkaProducer(bootstrap_servers='localhost:9092');

string_val = b'HELLO';

for _ in range(50):
    for i in range(0, 15):
        future = producer.send('test2', string_val, partition=i);
        try:
            record_metadata = future.get(timeout=10);
        except:
            log.exeception();
            pass


