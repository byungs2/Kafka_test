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

    def commit_partition(self, key):
        self.consumer.commit({ key : OffsetAndMetadata(offset=self.partial_offset[key] + 1, metadata=None)});
        self.partial_offset[key] += 1;
        print('commit_partition: ',self.partial_offset[key], key);

    def commit_partitions(self):
        print("commit partitionss");
        for key in self.keys:
            self.commit_partition(key);

    def seek_last_commit(self, key):
        commit = self.consumer.committed(key) + 1 if self.consumer.committed(key) else 0;
        self.consumer.seek(key, commit);
        self.partial_offset[key] = commit;
        print('Last commit: ', commit);

    def seek_last_commits(self):
        for key in self.keys:
            self.seek_last_commit(key);

    def fetch_message(self):
        self.seek_last_commits();
        for msg in self.consumer:
            topic_obj = TopicPartition(msg.topic, msg.partition);
            if(self.flag == self.complete_flag):
                print("#### Goto next step ####");
                self.flag = 0x00;
                self.commit_partitions();

            if(msg.offset == self.partial_offset[topic_obj] + 1):
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
            topics=[TopicPartition('test', 0),
                TopicPartition('test', 1)],
            tolerance=50,
            timeout=1000,
            fps=15);
    processor.polling_start();
    processor.fetch_message();



