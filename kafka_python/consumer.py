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
        self.init_flag = 0x00;
        self.flag = 0x00;

    def polling_start(self):
        self.consumer.poll(update_offsets=False);

    def commit_partition(self, key, dest_offset):
        self.consumer.commit({ key : OffsetAndMetadata(offset=dest_offset, metadata=None)});
        self.partial_offset[key] = dest_offset;

    def commit_partitions(self, dest_offset):
        for key in self.keys:
            self.commit_partition(key, dest_offset);

    def commit_to_zero(self):
        for key in self.keys:
            self.commit_partition(key, 0);

    def seek_last_commit(self, key):
        commit = self.consumer.committed(key) if self.consumer.committed(key) else 0;
        self.consumer.seek(key, commit + 1);
        #self.partial_offset[key] = commit;
        print('Last commit: ', commit);

    def seek_last_commits(self):
        for key in self.keys:
            self.seek_last_commit(key);

    def fetch_message_as_normal(self):
        self.commit_to_zero();
        self.seek_last_commits();
        for msg in self.consumer:
            print(msg.partition, msg.offset, msg.value);

    def fetch_message(self):
        self.commit_to_zero();
        self.commit_partitions(6700);
        self.seek_last_commits();
        for msg in self.consumer:
            topic_obj = TopicPartition(msg.topic, msg.partition);
            if(self.flag == self.complete_flag):
                print("#### Goto next step ####");
                self.flag = 0x00;
                self.commit_partitions(self.partial_offset[topic_obj] + 1);

            elif(msg.offset == self.partial_offset[topic_obj] + 1):
                self.flag = self.flag | (1 << msg.partition);
                self.seek_last_commit(topic_obj);
                print(msg.partition,
                        msg.offset,
                        self.flag, 
                        self.consumer.committed(topic_obj), 
                        self.partial_offset[topic_obj]);
            else :
                print("#### FALSE LOGS ####", msg.partition, msg.offset, self.partial_offset[topic_obj] + 1);

if __name__ == '__main__':
    processor = ConsumeWrapper(group_id='cons1',
            number_of_max_record=1,
            auto_commit=False,
            boot_server='localhost:9092',
            topics=[TopicPartition('test', 0),
                TopicPartition('test', 1),
                TopicPartition('test', 2),
                TopicPartition('test', 3),
                TopicPartition('test', 4),
                TopicPartition('test', 5),
                TopicPartition('test', 6),
                TopicPartition('test', 7),
                TopicPartition('test', 8),
                TopicPartition('test', 9),
                TopicPartition('test', 10),
                TopicPartition('test', 11),
                TopicPartition('test', 12),
                TopicPartition('test', 13),
                TopicPartition('test', 14)],
            tolerance=50,
            timeout=1000,
            fps=15);

    processor.polling_start();
    processor.fetch_message();
    #processor.fetch_message_as_normal();



