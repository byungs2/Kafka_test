from kafka import KafkaAdminClient
from kafka.admin import NewPartitions
from kafka.admin import NewTopic

admin = KafkaAdminClient(bootstrap_servers='localhost:9092');

if __name__ == '__main__':
    print(admin.list_consumer_groups());
    print(admin.list_consumer_group_offsets('cons1'));
    #admin.delete_topics(['test2']);
    admin.create_topics([NewTopic(name='test2', num_partitions=15, replication_factor=1)]);

