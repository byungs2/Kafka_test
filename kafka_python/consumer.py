from kafka import KafkaConsumer
from kafka import TopicPartition
from kafka import OffsetAndMetadata

class ConsumeWrapper:
    def __init__(self, **args):
        self.consumer = KafkaConsumer(group_id=args['group_id'],
                max_poll_records=args['number_of_max_record'],
                enable_auto_commit=args['auto_commit'],
                bootstrap_servers=args['boot_server']);
        self.consumer.assign(args['topics']);
        self.keys = args['topics'];
        self.partial_offset = {}; # partial consumer offset information
        self.tolerance = args['tolerance']; # tolerance of time difference
        self.packet_time = 0; # expected packet time
        self.timeout = args['timeout']; # timeout facotr for wating late packet
        self.fps = args['fps'];
        self.complete_flag = (1 << len(args['topics'])) - 1;
        self.flag = 0x00;

    def polling_start(self):
        self.consumer.poll(update_offsets=False);

    def commit_partition(self, key, dest_offset):
        self.consumer.commit({ key : OffsetAndMetadata(offset=dest_offset, metadata=None)});
        self.partial_offset[key] = dest_offset;

    def commit_partitions(self):
        for key in self.keys:
            self.commit_partition(key, self.partial_offset[key]+1);

    def commit_to_zero(self):
        for key in self.keys:
            self.commit_partition(key, 0);

    def seek_last_commit(self, key):
        commit = self.consumer.committed(key) if self.consumer.committed(key) else 0;
        self.consumer.seek(key, commit);
        self.partial_offset[key] = commit;
        print('Last commit: ', commit);

    def seek_last_commits(self):
        for key in self.keys:
            self.seek_last_commit(key);

    def fetch_message_as_normal(self):
        self.commit_to_zero();
        self.seek_last_commits();
        for msg in self.consumer:
            print(msg.partition, msg.offset);

    def fetch_message(self):
        self.commit_to_zero();
        self.seek_last_commits();
        for msg in self.consumer:
            topic_obj = TopicPartition(msg.topic, msg.partition);
            if(self.flag == self.complete_flag):
                print("#### Goto next step ####");
                self.flag = 0x00;
                self.commit_partitions();

            elif(msg.offset == self.partial_offset[topic_obj] + 1):
                self.seek_last_commit(topic_obj);
                self.flag = self.flag | (1 << msg.partition);
                print(msg.partition, 
                        self.flag, 
                        self.consumer.committed(topic_obj), 
                        self.partial_offset[topic_obj]);

if __name__ == '__main__':
    processor = ConsumeWrapper(group_id='cons1',
            number_of_max_record=1,
            auto_commit=False,
            boot_server='localhost:9092',
            topics=[TopicPartition('test2', 0),
                TopicPartition('test2', 1),
                TopicPartition('test2', 2),
                TopicPartition('test2', 3),
                TopicPartition('test2', 4),
                TopicPartition('test2', 5),
                TopicPartition('test2', 6),
                TopicPartition('test2', 7),
                TopicPartition('test2', 8),
                TopicPartition('test2', 9),
                TopicPartition('test2', 10),
                TopicPartition('test2', 11),
                TopicPartition('test2', 12),
                TopicPartition('test2', 13),
                TopicPartition('test2', 14)],
            tolerance=50,
            timeout=1000,
            fps=15);
    processor.polling_start();
    processor.fetch_message();
    #processor.fetch_message_as_normal();



