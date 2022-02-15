from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers='localhost:9092');
for _ in range(50):
    producer.send('test', b'Python_1', partition=0);
    producer.send('test', b'Python_2', partition=1);
